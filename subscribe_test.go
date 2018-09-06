package fullcache

import (
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-xorm/xorm"
	"gopkg.in/redis.v5"
)

func TestBinlogListener_getTableSchema(t *testing.T) {

	host := "127.0.0.1"
	port := 3306
	user := "root"
	password := ""
	db := "supplier"
	//file := "mysql-bin.000003"
	//pos := 7066

	bl := InitBinlogListener(100, host, port, db, user, password,  os.Stdout, os.Stderr)
	ts := bl.getTableSchema("supplier", "cron_record")
	t.Logf("%+v", ts)

}

func TestBinlogListener(t *testing.T) {
	host := "127.0.0.1"
	port := 3306
	user := "root"
	password := ""
	db := "supplier"
	//file := "mysql-bin.000003"
	//pos := 7066

	bl := InitBinlogListener(100, host, port, db, user, password,  os.Stdout, os.Stderr)
	cache := InitPKCache("supplier", bl)
	dbConn, err := xorm.NewEngine("mysql", "root:@tcp(127.0.0.1:3306)/supplier?charset=utf8")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	r := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	rc := NewRedisKV(dbConn, r, "dev_accounts", onMissDevAccount)
	cache.NewICache(rc, "dev_accounts")
	cache.Register(bl)
	t.Log(cache.Get("dev_accounts", "6"))
	dbConn.Table("dev_accounts").Where("id = 6").Update(map[string]interface{}{"token": "222"})
	time.Sleep(4 * time.Second)
	t.Log(cache.Get("dev_accounts", "6"))

}

func onMissDevAccount(db *xorm.Session, key string) (value string, err error) {
	user := new(DevAccounts)
	exists, err := db.Where("id = ?", key).Get(user)
	if err != nil {
		return value, err
	}
	if !exists {
	}
	value = string(jsonEncode(user))
	return
}

type DevAccounts struct {
	Id        int       `xorm:"int pk notnull unique autoincr 'id'"`
	UserId    int       `xorm:"int"`
	Username  string    `xorm:"varchar(32)"`
	Token     string    `xorm:"varchar(32) notnull"`
	CreatedAt time.Time `xorm:" TIMESTAMP 'created_at'"`
	UpdatedAt time.Time `xorm:" TIMESTAMP 'updated_at'"`
}

func jsonEncode(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func jsonDecode(b []byte, v interface{}) {
	json.Unmarshal(b, v)
}

func Test_toString(t *testing.T) {
	testcases := []interface{}{
		"1",
		2,
		false,
		time.Now(),
	}
	for _, tc := range testcases {
		t.Log(toString(tc))

	}

}

func Test_ch(t *testing.T) {
	var a int32 = 1
	success := atomic.CompareAndSwapInt32(&a, 0, 1)
	t.Log(a, success)
	success = atomic.CompareAndSwapInt32(&a, 0, 2)
	t.Log(a, success)
	success = atomic.CompareAndSwapInt32(&a, 1, 3)
	t.Log(a, success)

}

func TestBinlogListener_updateStatus(t *testing.T) {
	host := "127.0.0.1"
	port := 3306
	user := "root"
	password := ""
	db := "supplier"

	bl := InitBinlogListener(100, host, port, db, user, password, os.Stdout, os.Stderr)
	t.Log(masterStatus(bl.db))
}
