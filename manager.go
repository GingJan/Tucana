package tucana

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"runtime"
	"strings"
)

var mgr *TCache

type TCache struct {
	*manager
}

// Init The id parameter represents the identifier of caller
func Init(id string, rds redis.Conn) {
	m := newManager(id, rds)
	mgr = &TCache{m}
	runWatching(m)
	runtime.SetFinalizer(mgr, stopWatching)
}

//manager 负责维护rds连接，pubsub订阅，监控key值变更，以及通过channel下发通知给其他cache对象
type manager struct {
	id         string
	rds        redis.Conn
	pubSubConn redis.PubSubConn //pubsub会占一条连接

	subChans []chan alteration

	watcher *watcher
}

//变动的订阅消息
type alteration struct {
	oper operation
	key  string
}

func newManager(id string, rds redis.Conn) *manager {
	return &manager{
		id:         id,
		rds:        rds,
		pubSubConn: redis.PubSubConn{Conn: rds},
		subChans:   make([]chan alteration, 0),
		watcher:    nil,
	}
}

//注册cache的通知channel
func (m *manager) register(wc chan alteration) {
	m.subChans = append(m.subChans, wc)
}

func (m *manager) getChannelName() string {
	return fmt.Sprintf(updatingChanName, m.id)
}

func (m *manager) getKeyAndOperation(channelName string) alteration {
	s := strings.Split(channelName, "|")
	return alteration{
		oper: operation(s[1]),
		key:  s[0],
	}
}

// NotifyUpdating Notify the local cache on the other machines that the value of the key has changed
func (m *manager) NotifyUpdating(key string) error {
	//通知redis（发布订阅），更新local 和 redis 缓存
	_, err := m.rds.Do("PUBLISH", m.getChannelName(), fmt.Sprintf(chanMessageFormat, key, commandDel))
	return err
}
