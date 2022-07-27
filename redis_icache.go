package fullcache

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
	"xorm.io/xorm"
)

const redisTimeoutDefault = 5 * time.Second
const OneYear = 24 * 365 * time.Hour

type RedisCache struct {
	db     *xorm.Engine
	c      *redis.Client
	prefix string
	onMiss func(db *xorm.Session, key string) (value string, err error)
}

func NewRedisKV(db *xorm.Engine, r *redis.Client, prefix string, onMiss func(db *xorm.Session,
	key string) (value string,
	err error)) *RedisCache {
	return &RedisCache{
		db:     db,
		c:      r,
		prefix: prefix,
		onMiss: onMiss,
	}
}

func (r *RedisCache) Get(key string) (value string, err error) {
	redisKey := r.K(key)
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeoutDefault)
	defer cancel()
	value, err = r.c.Get(ctx, redisKey).Result()
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
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeoutDefault)
	defer cancel()
	_, err := r.c.Set(ctx, key, value, OneYear).Result()
	return err
}

func (r *RedisCache) Del(key string) (err error) {
	key = r.K(key)
	ctx, cancel := context.WithTimeout(context.Background(), redisTimeoutDefault)
	defer cancel()
	_, err = r.c.Del(ctx, key).Result()
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

func (r *RedisCache) K(key string) string {
	return r.prefix + ":" + key
}
