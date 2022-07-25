package fullcache

import (
	"github.com/go-xorm/xorm"
	"net/http"
	"time"
	"io/ioutil"
	"log"
	"encoding/json"
	"io"
	"strings"
	"github.com/pkg/errors"
)

const DefaultInternal = 30 * time.Second
const SearchTimeOut = "请求搜索超时"

type EsCache struct {
	db     *xorm.Engine
	esUrl  string // es node url
	index  string // es _index
	typ    string // es _type
	onMiss func(db *xorm.Session, key string) (value string, err error)
}

func NewEsKV(db *xorm.Engine, esUrl, index, typ string, onMiss func(db *xorm.Session, key string) (value string, err error)) *EsCache {
	return &EsCache{
		db:     db,
		esUrl:  esUrl,
		index:  index,
		typ:    typ,
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
	EsCreated = "created"
	EsUpdated = "updated"
	EsDeleted = "deleted"
)

func (es *EsCache) Get(key string) (value string, err error) {
	url := es.K(key)
	body, err := httpRequest(http.MethodGet, url, nil, nil)
	if err != nil {
		return value, err
	}
	var resp EsGetResp
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return value, err
	}
	source, err := json.Marshal(resp.Source)
	if err != nil {
		return value, err
	}
	value = string(source)
	if resp.Found { // 存在
		return value, nil
	}
	// 不存在，则查找插入
	value, err = es.OnMiss(key)
	if err != nil {
		return value, err
	}
	return
}

func (es *EsCache) Del(key string) (err error) {
	url := es.K(key)
	body, err := httpRequest(http.MethodDelete, url, nil, nil)
	if err != nil {
		return err
	}
	var resp EsUpdateResp
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return err
	}
	if resp.Result != EsDeleted { // deleted
		return errors.New("Del failed.")
	}
	return err
}

func (es *EsCache) Set(key, value string) error {
	url := es.K(key)
	body, err := httpRequest(http.MethodPost, url, nil, strings.NewReader(value))
	if err != nil {
		return err
	}
	var resp EsUpdateResp
	err = json.Unmarshal(body, &resp)
	if err != nil {
		return err
	}
	if resp.Result != EsCreated && resp.Result != EsUpdated { // created/updated
		return errors.New("Set Failed.")
	}
	return err
}

func (es *EsCache) OnMiss(key string) (value string, err error) {
	value, err = es.onMiss(es.db.NewSession(), key)
	if err != nil {
		return "", err
	}
	err = es.Set(key, value)
	return
}

func (es *EsCache) K(key string) string {
	return es.esUrl + "/" + es.index + "/" + es.typ + "/" + key
}

func httpRequest(method, url string, header map[string]string, body io.Reader) (result []byte, err error) {
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range header {
		req.Header.Set(k, v)
	}
	ch := make(chan error)
	go func() {
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			log.Println(err)
			ch <- err
		}
		if resp.Body != nil {
			defer resp.Body.Close()
		}
		result, err = ioutil.ReadAll(resp.Body)
		if err != nil {
			log.Println(err)
			ch <- err
		}
		ch <- nil
	}()
	select {
	case err := <-ch:
		return result, err
	case <-time.After(DefaultInternal):
		return result, errors.New(SearchTimeOut)
	}
	return
}
