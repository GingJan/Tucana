package tucana

import (
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

func NewRedisPool(server, password string, maxIdle, idleTimeout, maxActive int, options ...redis.DialOption) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     maxIdle,
		IdleTimeout: time.Duration(idleTimeout) * time.Second,
		MaxActive:   maxActive,
		Dial: func() (redis.Conn, error) {
			var c redis.Conn
			var err error
			protocol := "tcp"
			if strings.HasPrefix(server, "unix://") {
				server = strings.TrimLeft(server, "unix://")
				protocol = "unix"
			}
			c, err = redis.Dial(protocol, server, options...)
			if err != nil {
				return nil, err
			}
			if len(password) != 0 {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}
