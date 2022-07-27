package fullcache

import (
	"context"
	"github.com/olivere/elastic/v7"
	"github.com/pkg/errors"
	"time"
	"xorm.io/xorm"
)

const DefaultInternal = 30 * time.Second
const SearchTimeOut = "请求搜索超时"

type EsCache struct {
	db     *xorm.Engine
	es     *elastic.Client
	index  string // es _index
	onMiss func(db *xorm.Session, key string) (value string, err error)
}

func NewEsKV(db *xorm.Engine, es *elastic.Client, index string, onMiss func(db *xorm.Session, key string) (value string, err error)) *EsCache {
	return &EsCache{
		db:     db,
		es:     es,
		index:  index,
		onMiss: onMiss,
	}
}

/*
GET
{
    "_index": "warehouse",
    "_type": "order",
    "_id": "43020156",
    "found": false
}
*/
type EsGetResp struct {
	Id     string      `json:"_id"`
	Found  bool        `json:"found"`
	Source interface{} `json:"_source"`
}

type EsUpdateResp struct {
	Id     string `json:"_id"`
	Result string `json:"result"`
}

const (
	EsCreated        = "created"
	EsUpdated        = "updated"
	EsDeleted        = "deleted"
	esTimeoutDefault = 30 * time.Second
)

func (es *EsCache) Get(key string) (value string, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), esTimeoutDefault)
	defer cancel()
	res, err := es.es.Get().Index(es.index).Id(key).Do(ctx)
	if err != nil {
		return "", err
	}
	if res.Found {
		body, err := res.Source.MarshalJSON()
		if err != nil {
			return "", err
		}
		return string(body), nil
	}
	// 不存在，则查找插入
	value, err = es.OnMiss(key)
	if err != nil {
		return value, err
	}
	return
}

func (es *EsCache) Del(key string) (err error) {
	ctx, cancel := context.WithTimeout(context.Background(), esTimeoutDefault)
	defer cancel()
	res, err := es.es.Delete().Index(es.index).Id(key).Do(ctx)
	if err != nil {
		return err
	}
	if res.Result != EsDeleted {
		return errors.New("del failed")
	}
	return nil
}

func (es *EsCache) Set(key, value string) error {
	ctx, cancel := context.WithTimeout(context.Background(), esTimeoutDefault)
	defer cancel()
	res, err := es.es.Index().Index(es.index).Id(key).BodyString(value).Do(ctx)
	if err != nil {
		return err
	}
	if res.Result != EsCreated && res.Result != EsUpdated { // created/updated
		return errors.New("set failed")
	}
	return nil
}

func (es *EsCache) OnMiss(key string) (value string, err error) {
	value, err = es.onMiss(es.db.NewSession(), key)
	if err != nil {
		return "", err
	}
	err = es.Set(key, value)
	return
}
