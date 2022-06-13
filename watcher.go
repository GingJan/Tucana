package tucana

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
	"time"
)

type watcher struct {
	interval time.Duration
	stop     chan struct{}
	err      error
}

//创建并运行watcher，监控key值变化
func runWatching(m *manager) {
	w := &watcher{
		interval: 1 * time.Second,
		stop:     make(chan struct{}),
	}

	// first, we need to subscribe to the channel
	w.err = m.pubSubConn.Subscribe(m.getChannelName())
	m.watcher = w

	// then run the watching task
	go w.Run(m)
}

func stopWatching(c *TCache) {
	c.watcher.stop <- struct{}{}
}

// Run Watching all keys in local cache and update it if the key's value has changed
func (w *watcher) Run(m *manager) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-w.stop:
			for _, wc := range m.watchCs {
				close(wc)
			}
		case tm := <-ticker.C:
			//TODO 判断是否有subscribe channel

			tmd := strconv.FormatInt(tm.UnixNano(), 10)
			if err := m.pubSubConn.Ping(tmd); err != nil {
				log.Printf("watch ping time=%s, err=%s", tm.Format("2006-01-02 15:04:05"), err)
			}
		default:
			fmt.Println("blocking？")
			switch v := m.pubSubConn.ReceiveWithTimeout(1 * time.Second).(type) {
			case redis.Message:
				fmt.Printf("%s: message: %s\n", v.Channel, v.Data)

				if v.Channel != m.getChannelName() {
					continue
				}

				alteration := m.getKeyAndOperation(string(v.Data))
				for _, c := range m.watchCs {
					c <- alteration
				}

			case redis.Pong:
				nowUnix := time.Now().UnixNano()
				at, _ := strconv.ParseInt(v.Data, 10, 64)
				fmt.Printf("ping at=%d, now=%d, diff=%d", at, nowUnix, nowUnix-at)
			case error:
				fmt.Println(fmt.Sprintf("%#v", v))
				fmt.Printf("Run err=%s", v)
			}
		}
	}
}
