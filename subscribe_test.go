package fullcache

import (
	"encoding/json"
	"github.com/go-redis/redis/v8"
	_ "github.com/go-sql-driver/mysql"
	"github.com/olivere/elastic/v7"
	"github.com/olivere/elastic/v7/config"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
	"xorm.io/xorm"
)

func TestBinlogListener_getTableSchema(t *testing.T) {

	host := "127.0.0.1"
	port := 3306
	user := "root"
	password := "123456"
	db := "supplier"
	//file := "mysql-bin.000003"
	//pos := 7066

	bl := InitBinlogListener(100, host, port, db, user, password, os.Stdout, os.Stderr)
	ts := bl.getTableSchema("supplier", "cron_record")
	t.Logf("%+v", ts)

}

func TestBinlogListener(t *testing.T) {
	host := "127.0.0.1"
	port := 3306
	user := "root"
	password := "123456"
	db := "supplier"
	//file := "mysql-bin.000003"
	//pos := 7066

	bl := InitBinlogListener(100, host, port, db, user, password, os.Stdout, os.Stderr)
	cache := InitPKCache("supplier", bl)
	dbConn, err := xorm.NewEngine("mysql", "root:123456@tcp(127.0.0.1:3306)/supplier?charset=utf8")
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
	password := "123456"
	db := "supplier"

	bl := InitBinlogListener(100, host, port, db, user, password, os.Stdout, os.Stderr)
	t.Log(masterStatus(bl.db))
}

func TestBinlogListenerES(t *testing.T) {

	// 1. 初始化binlog 开启监听
	host := "127.0.0.1"
	port := 3306
	user := "root"
	password := "123456"
	db := "supplier"
	//file := "mysql-bin.000003"
	//pos := 7066

	bl := InitBinlogListener(100, host, port, db, user, password, os.Stdout, os.Stderr)
	cache := InitPKCache("supplier", bl)
	dbConn, err := xorm.NewEngine("mysql", "root:123456@tcp(127.0.0.1:3306)/supplier?charset=utf8")
	assert.NoError(t, err)
	dbConn.ShowSQL(true)
	// 2. 查询指定测试数据并注册订单表以及对应的ES的索引类型
	var testOrder OrdersAndPackageOrder
	_, err = dbConn.Table("sale_orders").Select("id, order_id, status_code, account_code, deleted, updated, created").Get(&testOrder)
	assert.NoError(t, err)
	es, err := elastic.NewClientFromConfig(&config.Config{URL: "http://127.0.0.1:9200", Sniff: &[]bool{false}[0]})
	assert.NoError(t, err)
	rc := NewEsKV(dbConn, es, "warehouse", onMissSaleOrders)
	cache.NewICache(rc, "sale_orders")
	cache.Register(bl)
	// 3. 获取订单与数据库查询的订单进行比较
	value, err := cache.Get("sale_orders", strconv.FormatInt(testOrder.Id, 10))
	assert.NoError(t, err)
	t.Log("get cache from es: ", value)
	var cacheOrder OrdersAndPackageOrder
	err = json.Unmarshal([]byte(value), &cacheOrder)
	assert.NoError(t, err)
	assert.Equal(t, testOrder.OrderId, cacheOrder.OrderId)

	// 4. 更新数据库的订单并获取ES订单，测试是否成功更新ES订单
	dbConn.Table("sale_orders").Where("id = ?", strconv.FormatInt(testOrder.Id, 10)).Update(map[string]interface{}{"order_id": testOrder.OrderId + "_test"})
	time.Sleep(2 * time.Second)
	value, err = cache.Get("sale_orders", strconv.FormatInt(testOrder.Id, 10))
	assert.NoError(t, err)
	t.Log("get cache from es after change data: ", value)
	cacheOrder = OrdersAndPackageOrder{}
	json.Unmarshal([]byte(value), &cacheOrder)
	assert.NoError(t, err)
	assert.Equal(t, testOrder.OrderId+"_test", cacheOrder.OrderId)

	// 5. 恢复数据库订单
	dbConn.Table("sale_orders").Where("id = ?", strconv.FormatInt(testOrder.Id, 10)).Update(map[string]interface{}{"order_id": testOrder.OrderId})
	time.Sleep(5 * time.Second)
}

// ES 不存在则查找数据库
func onMissSaleOrders(db *xorm.Session, key string) (value string, err error) {
	var oid int64
	oid, err = strconv.ParseInt(key, 10, 64)
	order := new(OrdersAndPackageOrder)
	exists, err := db.Table("sale_orders").Alias("so").Join("left", []string{"p_packages_and_order", "po"}, "po.order_num = so.order_id").
		Join("left", []string{"sale_orders_details", "sod"}, "so.order_id = sod.order_id").
		Select("so.id, so.account_code, so.platform, so.status_code, "+
			"so.order_id, so.created_at, so.email, so.name, so.phone1, so.phone2, so.address1, "+
			"so.address2, so.city, so.state, so.country, so.postal_code, so.platform_seller_id, so.deleted, so.src, "+
			"so.shipping_code, so.created, so.updated, so.main_order, so.buyer_id, "+
			"group_concat(distinct po.package_num) as package_num, group_concat(distinct sod.sku) as sku, group_concat(distinct sod.item_id) as item_id, "+
			"group_concat(distinct sod.transaction_id) as transaction_id ").
		GroupBy("so.id").Where("so.id=?", oid).Get(order)
	if err != nil {
		panic(err)
		return value, err
	}
	if !exists {
		log.Println("不存在该订单")
		return value, errors.New("不存在该订单")
	}
	value = string(jsonEncode(order))
	return value, nil
}

//server-uuid=653e0d8e-d740-11e8-9b53-b48fb0c6cfbd
// 测试同步时间间隔
func TestBinlogSyncTime(t *testing.T) {
	// 1. 初始化binlog 开启监听
	host := "127.0.0.1"
	port := 3306
	user := "root"
	password := "123456"
	db := "supplier"
	//file := "mysql-bin.000003"
	//pos := 7066

	bl := InitBinlogListener(100, host, port, db, user, password, os.Stdout, os.Stderr)
	cache := InitPKCache("supplier", bl)
	dbConn, err := xorm.NewEngine("mysql", "root:123456@tcp(127.0.0.1:3306)/supplier?charset=utf8")
	assert.NoError(t, err)
	// 2. 查询指定测试数据并注册订单表以及对应的ES的索引类型
	var testOrder OrdersAndPackageOrder
	_, err = dbConn.Table("sale_orders").Select("id, order_id, status_code, account_code, deleted, updated, created").Get(&testOrder)
	assert.NoError(t, err)
	es, err := elastic.NewClientFromConfig(&config.Config{URL: "http://127.0.0.1:9200", Sniff: &[]bool{false}[0]})
	assert.NoError(t, err)
	rc := NewEsKV(dbConn, es, "warehouse", onMissSaleOrders)
	cache.NewICache(rc, "sale_orders")
	cache.Register(bl)
	// 2. 查询指定测试数据并注册订单表以及对应的ES的索引类型
	_, err = dbConn.Table("sale_orders").Select("id, order_id, status_code, account_code, deleted, updated, created").Get(&testOrder)
	assert.NoError(t, err)
	t.Log(testOrder.Id, testOrder.OrderId)
	interval := 1750 * time.Millisecond // 时间间隔
	fail := 0
	for interval > 0 {
		t.Log("Interval ", interval)
		updatedOrderId := strconv.FormatInt(time.Now().Unix(), 10)
		dbConn.Table("sale_orders").Where("id = ?", strconv.FormatInt(testOrder.Id, 10)).Update(map[string]interface{}{"order_id": updatedOrderId})
		time.Sleep(interval)
		value, err := cache.Get("sale_orders", strconv.FormatInt(testOrder.Id, 10))
		assert.NoError(t, err)
		var esOrder OrdersAndPackageOrder
		err = json.Unmarshal([]byte(value), &esOrder)
		assert.NoError(t, err)
		if updatedOrderId != esOrder.OrderId {
			t.Log("同步失败")
			fail++
			if fail >= 10 {
				t.Fatal("失败10次")
			} else {
				interval = interval + 30*time.Millisecond
			}
		} else {
			t.Log("同步成功")
		}
		interval = interval - 20*time.Millisecond

		// recovery
		dbConn.Table("sale_orders").Where("id = ?", strconv.FormatInt(testOrder.Id, 10)).Update(map[string]interface{}{"order_id": testOrder.OrderId})
		time.Sleep(3 * time.Second)
	}
}

type OrdersAndPackageOrder struct {
	Id               int64 `xorm:"id"`
	StatusCode       int
	Deleted          int
	Platform         string
	OrderId          string
	AccountCode      string
	Address1         string
	Address2         string
	City             string
	State            string
	Country          string
	Email            string
	Name             string
	Phone1           string
	Phone2           string
	PostalCode       string
	Src              string
	ShippingCode     string
	LikeStr          string    `xorm:"-"` // 用来辅助查询的字符串
	CreatedAt        time.Time `json:"-"`
	Created          time.Time `xorm:"created" json:"-"`
	Updated          time.Time `xorm:"updated" json:"-"`
	CreatedAtTs      int64     `xorm:"-" json:"created_at"`
	CreatedTs        int64     `xorm:"-" json:"created"`
	UpdatedTs        int64     `xorm:"-" json:"updated"`
	PackageNum       string    `xorm:"package_num"`
	ItemId           string    `xorm:"item_id" json:"-"` // +item_id, transaction_id buyer_id zyq20181227
	TransactionId    string    `xorm:"transaction_id" json:"-"`
	BuyerId          string    `xorm:"buyer_id"`
	Sku              string    `json:"-"`
	PlatformSellerId string
	MainOrder        string
}
