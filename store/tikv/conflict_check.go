package tikv

import (
	//"github.com/zond/gotomic"
	"sync"
	//log "github.com/Sirupsen/logrus"
	//"github.com/pingcap/tidb/kv"
	log "github.com/Sirupsen/logrus"
)

type conflictCheckTable struct {
	//hashTable *gotomic.Hash
	//hashTable map[string]interface{}
	waitList  map[string] *chan struct{}
	lock      sync.Mutex
}

var conflictTable *conflictCheckTable

func(c *conflictCheckTable) put(key []byte) *chan struct{} {
	lock := make(chan struct{}, 1)
	c.waitList[string(key)] = &lock
	log.Infof("[XUWT] put key(%s)", string(key))
	return &lock
}

func(c *conflictCheckTable) delete (key []byte) {
	delete(c.waitList, string(key))
	//log.Infof("[XUWT] delete key(%s)", string(key))
}

func(c *conflictCheckTable) get(key []byte) *chan struct{} {
	lock, ok := c.waitList[string(key)]
	if ok {
		return lock
	} else {
		return nil
	}
}

func checkConflict(keys [][]byte) *chan struct{}  {
	conflictTable.lock.Lock()
	defer conflictTable.lock.Unlock()
	for _, key := range keys {
		lock := conflictTable.get(key)
		if lock == nil {
			return conflictTable.put(key)
		} else {
			return lock
		}
	}
	return nil
}

func deleteKeys(keys [][]byte) {
	conflictTable.lock.Lock()
	defer conflictTable.lock.Unlock()
	for _, key := range keys {
		conflictTable.delete(key)
	}
}

func init() {
	conflictTable = &conflictCheckTable{
		waitList: make(map[string]*chan(struct{})),
	}
}
