package fullcache

import (
	"strings"
	"sync"
	"time"
	"sync/atomic"
	"errors"
)

type DispatchFunc func(table, pk string) error
// schema.table.PK -> data
type PKCache struct {
	shardIndex    map[string]uint16 // schema.table -> index of shard
	shards        []ICache
	ch            chan Msg
	toUpdate      chan string // tableNames to be updated
	schema        string
	tables        map[string]int32 // 限定这个 Cache 中需要管理哪些table, 不在这个table中的就不管了.  value 表示当前这个tables 中有几次更新
	tablesLock    *sync.RWMutex
	processGap    int
	updateRunning uint32
	dispatcher    map[string]DispatchFunc
}

func (c *PKCache) Register(bl *BinlogListener) {
	bl.Subscribe(c.ch)
}

func (c *PKCache) Dispatch(table string, f DispatchFunc) {
	if !strings.Contains(table, ".") { // 如果有 . 说明已经是 schema.table, 如果没有，说明只是table,需要拼接
		table = tableKey(c.schema, table)
	}
	c.dispatcher[table] = f
}

var ErrNil = errors.New("err: Nil Cache")
func (c *PKCache) Get(table, pk string) (data string, err error) {
	shard := c.getShard(table)
	if shard == nil {
		return "", ErrNil
	}
	return shard.Get(pk)
}

func (c *PKCache) Set(table, pk, data string) (err error) {
	shard := c.getShard(table)
	if shard == nil {
		return  ErrNil
	}
	return shard.Set(pk, data)
}

func (c *PKCache) Del(table, pk string) (err error) {
	shard := c.getShard(table)
	if shard == nil {
		return  ErrNil
	}
	return shard.Del(pk)
}

func (c *PKCache) Update(table, pk string) (err error) {
	shard := c.getShard(table)
	if shard != nil  {
		_, err = shard.OnMiss(pk)
	}

	return
}

func (c *PKCache) getShard(table string) ICache {
	table = tableName(c.schema, table)
	i, exists := c.shardIndex[table]
	if !exists {
		return nil
	}
	return c.shards[i]
}

const MaxQueueLen int = 64 // experiential
const BufferSize int = 256 // experiential
func (c *PKCache) subscribeLoop() {
	processTicker := time.NewTicker(time.Second * time.Duration(c.processGap))
	for {
		select {
		case msg := <- c.ch:
			switch msg.Type {
			case MsgUpdateRow:
				//fmt.Println("updateRow:", msg.Data)
				c.toUpdate <- msg.Data
				if len(c.toUpdate) >= MaxQueueLen {
					c.handleUpdate()
				}
			case MsgAlterTable:
			case MsgDeleteRow:
				table, pk, ret := c.parseMsg(msg.Data)
				if ret {
					c.Del(table, pk)
				}
				c.toUpdate <- ""
			}
		case <- processTicker.C:
			c.handleUpdate()
		}
	}
}

//
func (c *PKCache) handleUpdate() {
	success := atomic.CompareAndSwapUint32(&c.updateRunning, 0, 1)
	if !success {
		return // 有另一个任务还未完成
	}
	// 本次需要完成的数量
	workTimes := len(c.toUpdate)
	if workTimes > MaxQueueLen {
		workTimes = MaxQueueLen
	}
	updateMap := make(map[string]struct{})
	for i:=0; i<workTimes; i++ {
		data := <- c.toUpdate
		if _, exists := updateMap[data]; !exists {
			updateMap[data] = struct{}{}
		}
	}
	for data := range updateMap {
		table, pk, ret := c.parseMsg(data)
		if ret {
			c.Update(table, pk)
			if f, exists := c.dispatcher[table]; exists {
				//fmt.Println("displatch:", table)
				f(table, pk)
			}
		}
	}
	atomic.StoreUint32(&c.updateRunning, 0)
}

func (c *PKCache) NewICache(ic ICache, table string) {
	if _, exists:= c.tables[table]; !exists {
		c.tables[table] = 0
	}

	tKey := tableKey(c.schema, table)
	if _, exists := c.shardIndex[tKey]; !exists  {
		c.shards = append(c.shards, ic)
		c.shardIndex[tKey] = uint16(len(c.shards)-1)
	}
	//fmt.Printf("ICache: %+v \n", c.shardIndex)
}

func (c *PKCache) parseMsg(msg string) (tableName, pk string, result bool) {
	parts := strings.Split(msg, ".")
	if len(parts) != 3 {
		result = false
		return

	}
	if parts[0] != c.schema {
		result = false
		return
	}
	//if _, exists := c.tables[parts[1]]; !exists {
	//	result = false
	//	return
	//}
	tableName = tableKey(parts[0], parts[1])
	pk = parts[2]
	result = true
	return
}

func tableName(schema, table string) string{
	if strings.HasPrefix(table, schema+".") {
		return table
	} else {
		return tableKey(schema, table)
	}
}

func InitPKCache(schema string, bl *BinlogListener) *PKCache {
	pkc := PKCache{
		shardIndex:      make(map[string]uint16),
		shards: make([]ICache, 0),
		ch:     make(chan Msg, 0),
		toUpdate: make(chan string, BufferSize),
		schema: schema,
		tables: make(map[string]int32),
		updateRunning: 0,
		processGap: 2,
		dispatcher: make(map[string]DispatchFunc),
	}
	pkc.Register(bl)
	go pkc.subscribeLoop()
	return &pkc
}

