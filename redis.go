package redis

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Redis struct {
	pool *redis.Pool
}

// redis连接池
func (p *Redis) newPool(host string, port int, password string, maxConn, maxIdle int) *redis.Pool {
	return &redis.Pool{
		MaxActive:   maxConn,
		MaxIdle:     maxIdle,
		IdleTimeout: 10 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", fmt.Sprintf("%v:%v", host, port))
			if err != nil {
				return nil, err
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

// 初始化
func (p *Redis) Init(host string, port int, password string, maxConn, maxIdle int) error {
	p.pool = p.newPool(host, port, password, maxConn, maxIdle)
	if p.pool == nil {
		return errors.New("redis初始化失败！")
	}
	return nil
}

// 最后需要调用关闭连接
func (p *Redis) Close() error {
	return p.pool.Close()
}

func (p *Redis) GetString(db int, key string) (string, error) {
	return redis.String(p.Do(db, "GET", key))
}

func (p *Redis) GetInt(db int, key string) (int, error) {
	return redis.Int(p.Do(db, "GET", key))
}

func (p *Redis) GetInt64(db int, key string) (int64, error) {
	return redis.Int64(p.Do(db, "GET", key))
}

func (p *Redis) IsKeyExist(db int, key string) (int, error) {
	return redis.Int(p.Do(db, "EXISTS", key))
}

func (p *Redis) Do(db int, command string, args ...interface{}) (interface{}, error) {
	conn := p.pool.Get()
	defer conn.Close()
	conn.Do("select", db)
	return conn.Do(command, args...)
}

// hash设置多项
func (p *Redis) HMSet(db int, key string, values map[string]interface{}) error {
	args := []interface{}{key}
	for k, v := range values {
		args = append(args, k, v)
	}
	if len(args) == 1 {
		return fmt.Errorf("values 不允许为空")
	}

	_, err := p.Do(db, "HMSET", args...)
	return err
}

// 获取hash所有的值
func (p *Redis) HGetAll(db int, key string, v interface{}) (bool, error) {
	exist, err := redis.Bool(p.Do(db, "EXISTS", key))
	if err != nil || !exist {
		return exist, err
	}
	result, err := redis.Values(p.Do(db, "HGETALL", key))
	if err != nil {
		return true, err
	}
	if err := redis.ScanStruct(result, v); err != nil {
		return true, err
	}
	return true, nil
}

// 设置列表元素
func (p *Redis) LPUSH(db int, key string, v interface{}) error {
	if _, ok := v.(string); ok {
		_, err := p.Do(db, "LPUSH", key, v)
		return err
	} else {
		bytes, _ := json.Marshal(v)
		_, err := p.Do(db, "LPUSH", key, string(bytes))
		return err
	}
}

func (p *Redis) BRPOP(db int, key string, timeout int) (string, error) {
	arr, err := redis.Strings(p.Do(db, "BRPOP", key, timeout))
	if len(arr) == 2 {
		return arr[1], err
	}
	return "", err
}

func (p *Redis) LLEN(db int, key string) (int64, error) {
	result, err := redis.Int64(p.Do(db, "LLEN", key))
	return result, err
}

func (p *Redis) LRANGE(db int, key string, start, end int64) ([]string, error) {
	return redis.Strings(p.Do(db, "LRANGE", key, start, end))
}

func (p *Redis) LPOP(db int, key string) (string, error) {
	return redis.String(p.Do(db, "LPOP", key))
}

func (p *Redis) LSET(db int, key string, index int64, v interface{}) error {
	bytes, _ := json.Marshal(v)
	_, err := p.Do(db, "LSET", key, index, string(bytes))
	return err
}

func (p *Redis) LINDEX(db int, key string, index int64) (string, error) {
	return redis.String(p.Do(db, "LINDEX", key, index))
}

// 设置过期
func (p *Redis) SetExpire(db int, key string, sec int) error {
	_, err := p.Do(0, "EXPIRE", key, sec)
	return err
}

// 正则匹配keys
func (p *Redis) RegularKeys(db int, key string) ([]string, error) {
	return redis.Strings(p.Do(db, "KEYS", key))
}

func (p *Redis) DelRegularKeys(db int, key string) error {
	items, err := redis.Strings(p.Do(db, "KEYS", key))
	if err != nil {
		return err
	}
	for _, item := range items {
		if _, err := p.Do(db, "DEL", item); err != nil {
			return err
		}
	}
	return nil
}

func (p *Redis) ZADD(db int, key string, values map[string]interface{}) error {
	args := []interface{}{key}
	for member, score := range values {
		args = append(args, score, member)
	}
	if len(args) == 1 {
		return fmt.Errorf("values 不允许为空")
	}

	_, err := p.Do(db, "ZADD", args...)
	return err
}

func (p *Redis) ZCARD(db int, key string) (int64, error) {
	result, err := redis.Int64(p.Do(db, "ZCARD", key))
	return result, err
}

func (p *Redis) ZCOUNT(db int, key string, min, max int64) (int64, error) {
	result, err := redis.Int64(p.Do(db, "ZCOUNT", key, min, max))
	return result, err
}

func (p *Redis) ZRANGEBYSCORE(db int, key string, min, max int64) ([]string, error) {
	return redis.Strings(p.Do(db, "ZRANGEBYSCORE", key, min, max))
}

func (p *Redis) ZREMRANGEBYSCORE(db int, key string, min, max int64) (int64, error) {
	result, err := redis.Int64(p.Do(db, "ZREMRANGEBYSCORE", key, min, max))
	return result, err
}

func (p *Redis) ZREMRANGEBYRANK(db int, key string, min, max int64) (int64, error) {
	result, err := redis.Int64(p.Do(db, "ZREMRANGEBYRANK", key, min, max))
	return result, err
}

func (p *Redis) DELKey(db int, key string) error {
	_, err := p.Do(db, "DEL", key)
	return err
}

func (p *Redis) PUBLISH(db int, channel, msg string) error {
	_, err := p.Do(db, "PUBLISH", channel, msg)
	return err
}
