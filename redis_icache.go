package fullcache

import (
	"github.com/go-xorm/xorm"
	"gopkg.in/redis.v5"
	"time"
)

const OneYear = 24 * 365 * time.Hour

type RedisCache struct {
	db *xorm.Engine
	c *redis.Client
	prefix string
	onMiss func(db *xorm.Session, key string) (value string, err error)
}

func NewRedisKV(db *xorm.Engine, r *redis.Client, prefix string, onMiss func(db *xorm.Session,
	key string) (value string,
	err error)) *RedisCache {
	return &RedisCache{
		db:     db,
		c: r,
		prefix: prefix,
		onMiss: onMiss,
	}
}

func (r *RedisCache) Get(key string) (value string, err error) {
	redisKey := r.K(key)
	value, err =  r.c.Get(redisKey).Result()
	if err == redis.Nil {
		value, err = r.OnMiss(key)
		if err != nil {
			return value, err
		}
		return
	}
	return
}


func (r *RedisCache) Set(key, value string) error {
	key = r.K(key)
	//log.Info("RedisSet: " + key)
	_, err := r.c.Set(key, value, OneYear).Result()
	return err
}

func (r *RedisCache) Del(key string) (err error) {
	key = r.K(key)
	_, err = r.c.Del(key).Result()
	return err
}

func (r *RedisCache) OnMiss(key string) (value string, err error) {
	value, err = r.onMiss(r.db.NewSession(), key)
	if err != nil {
		return
	}
	err = r.Set(key, value)
	return
}

func (r *RedisCache) K(key string) string{
	return r.prefix + ":" + key
}

