package fullcache

import (
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go-mysql/mysql"
	"os"
	"context"
	"time"
	"io"
	"log"
	"database/sql"
	"strings"
	"sync"
	"reflect"
	"fmt"
	"strconv"
)

// Listener to and parse binlog
type BinlogListener struct {
	streamer *replication.BinlogStreamer
	database string
	infoLogger *log.Logger
	errorLogger *log.Logger
	tableMap map[uint64]string // id -> schema.tablename
	tsMap map[string]*TableSchema // key -> schema (key: tableKey(schema, name)
	subscribes []chan Msg
	tmLock *sync.RWMutex // tableMap's lock
	subLock *sync.RWMutex // subscribes's lock
	tsmLock *sync.RWMutex // tsMap's lock
	db *sql.DB
}


func InitBinlogListener(serverId int, host string, port int, db, user, password string, file string,
	pos int, iLogWriter, eLogWriter io.Writer) *BinlogListener {
	cfg := replication.BinlogSyncerConfig {
		ServerID: uint32(serverId),
		Flavor:   "mysql",
		Host:     host,
		Port:     uint16(port),
		User:     user,
		Password: password,
	}
	dbconn, err := dbConn(db, host, port, user, password)
	if err != nil {
		panic(err)
	}
	syncer := replication.NewBinlogSyncer(cfg)
	streamer, _ := syncer.StartSync(mysql.Position{file, uint32(pos)})
	bl :=  BinlogListener{
		streamer: streamer,
		database: db,
		infoLogger: log.New(iLogWriter, "fullcache:info", log.LstdFlags|log.Lshortfile),
		errorLogger: log.New(eLogWriter, "fullcache:error", log.LstdFlags|log.Lshortfile),
		db: dbconn,
		tableMap: make(map[uint64]string),
		tsMap: make(map[string]*TableSchema),
		tmLock: &sync.RWMutex{},
		tsmLock: &sync.RWMutex{},
		subLock: &sync.RWMutex{},
	}
	go bl.Loop()
	return &bl
}

func (bl *BinlogListener) Loop() {
	bl.infoLogger.Println("binlogListener loop on")
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		ev, err := bl.streamer.GetEvent(ctx)
		cancel()

		if err == context.DeadlineExceeded {
			// meet timeout
			continue
		}
		switch ev.Header.EventType {
		case replication.TABLE_MAP_EVENT:
			e, ok := ev.Event.(*replication.TableMapEvent)
			if !ok {
				bl.errorLogger.Println("TableMapEvent parse err")
			}
			schema := string(e.Schema)
			table := string(e.Table)
			bl.updateTable(schema, table, e.TableID, false)
		case replication.UPDATE_ROWS_EVENTv2:
			e, ok := ev.Event.(*replication.RowsEvent)
			if !ok {
				bl.errorLogger.Println("UpdateRowsEvent parse err")
			}
			tableName := bl.tableMap[e.TableID]
			ts := bl.tsMap[tableName]
			for i:=1; i<len(e.Rows); i+=2 {
				beforeRow := e.Rows[i-1]
				pks := make([]string, len(ts.PKIndex)) // 这一条数据里，所有是PK的field
				fmt.Println("------pks------")
				fmt.Println(ts.PKIndex)
				fmt.Println(beforeRow)
				for i:=0; i<len(ts.PKIndex); i++ {
					pki := ts.PKIndex[i]
					pks[i] = toString(beforeRow[pki])
				}
				pk := pkEncode(pks)
				msg := Msg{
					Type: MsgUpdateTable,
					Data: tableName + "." + pk,
				}
				go bl.Pub(msg)
			}
		}
		// TODO 还需要支持 table 更改， 删除等事件
		ev.Dump(os.Stdout)
	}
}

func (bl *BinlogListener) updateTable(schema, table string, tableId uint64, must bool) {
	tableName := tableKey(schema, table)

	bl.tsmLock.Lock()
	if _, exists := bl.tsMap[tableName]; must || !exists {
		ts := bl.getTableSchema(schema, table)
		bl.tsMap[tableName] = ts
	}
	bl.tsmLock.Unlock()
	bl.tmLock.Lock()
	if _, exists := bl.tableMap[tableId]; must || !exists {
		bl.tableMap[tableId] = tableName
	}
	bl.tmLock.Unlock()
}

func (bl *BinlogListener) Pub(msg Msg) {
	for _, ch := range bl.subscribes {
		ch <- msg
	}
}


func (bl *BinlogListener) getTableSchema(schema, table string) *TableSchema {
	name := tableKey(schema, table)
	sqlSchema := "DESCRIBE " + name
	rows, err := bl.db.Query(sqlSchema)
	if err != nil {
		bl.errorLogger.Println(err)
		return nil
	}
	pkIndex := make([]int, 0) //  index of PK
	fields := make([]string, 0)
	i := 0
	for rows.Next() {
		var field, t, null, key  string
		var ptr interface{}
		rows.Scan(&field, &t, &null, &key, &ptr, &ptr)
		fields = append(fields, field)
		if key == "PRI" {
			pkIndex = append(pkIndex, i)
		}
		i++
	}
	pkFields := make([]string, len(pkIndex))
	for i:=0; i<len(pkIndex); i++ {
		pkFields[i] = fields[pkIndex[i]]
	}
	pk := pkEncode(pkFields)
	ts := TableSchema{
		Fields: fields,
		PKIndex: pkIndex,
		PK: pk,
	}
	return &ts
}

func (bl *BinlogListener) Subscribe(ch chan Msg) {
	bl.subLock.Lock()
	bl.subscribes = append(bl.subscribes, ch)
	bl.subLock.Unlock()
}

// pkEncode: Encode Primary Key
func pkEncode(pkFields []string) string {
	fmt.Println("pkEncode: ", pkFields)
	switch len(pkFields) {
	case 0:
		return ""
	case 1:
		return pkFields[0]
	default:
		return  strings.Join(pkFields, "&")
	}
}

func pkDecode(pk string) (fields []string) {
	return strings.Split(pk, "&")
}


func tableKey(schema, table string) string {
	return schema+"."+table
}
type TableSchema struct {
	Fields []string
	PKIndex []int
	PK string
}

func toString(e interface{}) string {
	kind := reflect.ValueOf(e).Kind()
	fmt.Println(kind.String())
	switch kind {
	case reflect.String:
		return e.(string)
	case reflect.Bool:
		return strconv.FormatBool(e.(bool))
	case reflect.Int8:
		return strconv.FormatInt(int64(e.(int8)), 10)
	case reflect.Int16:
		return strconv.FormatInt(int64(e.(int16)), 10)
	case reflect.Int32:
		return strconv.FormatInt(int64(e.(int32)), 10)
	case reflect.Int:
		return strconv.FormatInt(int64(e.(int)), 10)
	case reflect.Int64:
		return strconv.FormatInt(int64(e.(int64)), 10)
	case reflect.Uint8:
		return strconv.FormatInt(int64(e.(uint8)), 10)
	case reflect.Uint16:
		return strconv.FormatInt(int64(e.(uint16)), 10)
	case reflect.Uint32:
		return strconv.FormatInt(int64(e.(uint32)), 10)
	case reflect.Uint:
		return strconv.FormatInt(int64(e.(uint)), 10)
	case reflect.Uint64:
		return strconv.FormatInt(int64(e.(uint64)), 10)
	default:
		return ""
	}
}