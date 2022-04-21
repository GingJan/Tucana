package tucana

import (
	"context"
	"fmt"
	"github.com/garyburd/redigo/redis"
	jsoniter "github.com/json-iterator/go"
	localCache "github.com/patrickmn/go-cache"
	"strconv"
	"sync"
	"time"
)

type CacheUpdateFunc func(ctx context.Context) (interface{}, error)

const (
	commandDel = "DEL"

	pingTicking = "PING_TICKING"
)

type Cache interface {
	GetSrc() //从源获取数据
}

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type TCache struct {
	localCache      *localCache.Cache
	rds             redis.Conn
	pubSubConn      redis.PubSubConn
	subscribingChan sync.Map
	tickers         map[string]time.Duration
}

func New(rds redis.Conn) *TCache {
	return &TCache{
		rds:             rds,
		pubSubConn:      redis.PubSubConn{Conn: rds},
		subscribingChan: sync.Map{},
		tickers:         make(map[string]time.Duration),
	}
}

func (t *TCache) GetLocal(key string, model interface{}) error {
	if raw, ok := t.localCache.Get(key); ok {
		data := raw.([]byte)
		if len(data) == 0 {
			return nil
		}
		return json.Unmarshal(data, model)
	}

	raw, err := redis.Bytes(t.rds.Do("GET", key))
	if err != nil {
		return err
	}

	if len(raw) == 0 {
		t.localCache.SetDefault(key, "")
	}

	err = json.Unmarshal(raw, model)
	if err != nil {
		return err
	}

	t.localCache.SetDefault(key, raw)

	return nil
}

func (t *TCache) NotifyUpdating(key string) error {
	//通知redis（发布订阅），更新local 和 redis 缓存
	channelName := t.getChannelName(key)
	if _, ok := t.subscribingChan.LoadOrStore(channelName, struct{}{}); !ok {
		t.pubSubConn.Subscribe(channelName)
	}

	_, err := t.rds.Do("PUBLISH", channelName, commandDel)
	return err
}

func (t *TCache) Update(ctx context.Context, key string, f CacheUpdateFunc) error {
	//执行f
	_, err := f(ctx)
	if err != nil {
		return err
	}

	return t.NotifyUpdating(key)
}

func (t *TCache) watch(ctx context.Context) error {
	var ok bool
	var tick time.Duration
	if tick, ok = t.tickers[pingTicking]; !ok {
		tick = 5 * time.Second
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		//select {
		//case <-ctx.Done():
		//	return ctx.Err()
		//case tm := <-ticker.C:
		//	//TODO 判断是否有subscribe channel
		//
		//	tmd := strconv.FormatInt(tm.UnixNano(), 10)
		//	if err := t.pubSubConn.Ping(tmd); err != nil {
		//		log.Printf("watch ping time=%s, err=%s", tm.Format("2006-01-02 15:04:05"), err)
		//	}
		//
		//}
		fmt.Println("blocking？")
		switch v := t.pubSubConn.ReceiveWithTimeout(0).(type) {
		case redis.Message:
			fmt.Printf("%s: message: %s\n", v.Channel, v.Data)
			key := t.getKey(v.Channel)
			t.rds.Do(string(v.Data), key)
			t.purgeLocal(key)
		case redis.Pong:
			nowUnix := time.Now().UnixNano()
			at, _ := strconv.ParseInt(v.Data, 10, 64)
			fmt.Printf("ping at=%d, now=%d, diff=%d", at, nowUnix, nowUnix-at)
		case error:
			return v
		}
	}
}

func (t *TCache) purgeLocal(key string) {
	t.localCache.Delete(key)
}

func (t *TCache) getChannelName(key string) string {
	return fmt.Sprintf("cnel:%s", key)
}

func (t *TCache) getKey(channelName string) string {
	return channelName[5:]
}
