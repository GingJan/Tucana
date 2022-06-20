package tucana

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
	"time"
)

type watcher struct {
	m        *manager
	interval time.Duration
	stop     chan struct{}
	err      error
}

//创建并运行watcher，监控key值变化
func runWatching(m *manager) {
	w := &watcher{
		m:        m,
		interval: 30 * time.Second,
		stop:     make(chan struct{}),
	}

	m.watcher = w
	// then run the watching task
	go w.Run()
}

func stopWatching(c *TCache) {
	c.manager.watcher.Close()
}

func (w *watcher) letsPing(t time.Time) {
	tmd := strconv.FormatInt(t.UnixNano(), 10)
	if err := w.m.pubSubConn.Ping(tmd); err != nil {
		log.Printf("send ping at=%s, err=%s", t.Format("2006-01-02 15:04:05"), err)
	}

	return
}

// Run Watching all keys in local cache and update it if the key's value has changed
func (w *watcher) Run() {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	// subscribe to the channel first.
	w.stare()

	go func() {
		for {
			select {
			case <-w.stop:
				w.shutdown()
			case tm := <-ticker.C:
				//TODO 判断是否有subscribe channel
				w.letsPing(tm)
			}
		}
	}()

	for {
		select {
		case <-w.stop:
			w.shutdown()
		default:
			switch v := w.m.pubSubConn.ReceiveWithTimeout(60 * time.Second).(type) {
			case redis.Subscription:
				fmt.Println(fmt.Sprintf("tucana has %sd channel", v.Kind, v.Channel))
			case redis.Message:
				fmt.Println(fmt.Sprintf("%s: message: %s\n", v.Channel, v.Data))

				if v.Channel != w.m.getChannelName() {
					continue
				}

				alteration, err := w.m.getKeyAndOperation(string(v.Data))
				if err != nil {
					continue
				}

				for _, c := range w.m.watchCs {
					c <- alteration
				}

			case redis.Pong:
				nowUnix := time.Now().UnixNano()
				at, _ := strconv.ParseInt(v.Data, 10, 64)
				fmt.Println(fmt.Sprintf("ping deferred=%dms", (nowUnix-at)/1e6))

			case error:
				//closing old conn
				w.release()

				//opening new conn
				w.stare()
			}
		}
	}
}

func (w *watcher) stare() {
	w.m.pubSubConn = redis.PubSubConn{Conn: w.m.rdsPool.Get()}
	w.err = w.m.pubSubConn.Subscribe(w.m.getChannelName())
}

func (w *watcher) Close() {
	close(w.stop)
}

func (w *watcher) release() {
	//closing conn
	w.m.pubSubConn.Unsubscribe(w.m.getChannelName())
	w.m.pubSubConn.Close()
}

func (w *watcher) shutdown() {
	//closing conn
	w.release()

	//closing all the tag cacher's chan
	for _, wc := range w.m.watchCs {
		close(wc)
	}
}
