package tikv

import (
	//"github.com/zond/gotomic"
	"sync"
	//log "github.com/Sirupsen/logrus"
	//"github.com/pingcap/tidb/kv"
)

type conflictCheckTable struct {
	//hashTable *gotomic.Hash
	//hashTable map[string]interface{}
	waitList  map[string] chan struct{}
	lock      sync.Mutex
}

var conflictTable *conflictCheckTable

func(c *conflictCheckTable) put(key []byte) chan struct{} {
	lock := make(chan struct{}, 1)
	c.waitList[string(key)] = lock
	return lock
	//log.Infof("[XUWT] put key(%s)", string(key))
}

func(c *conflictCheckTable) delete (key []byte) {
	delete(c.waitList, string(key))
	//log.Infof("[XUWT] delete key(%s)", string(key))
}

func(c *conflictCheckTable) get(key []byte) chan struct{} {
	lock, ok := c.waitList[string(key)]
	if ok {
		return lock
	} else {
		return nil
	}
}

func checkConflict(keys [][]byte) chan struct{}  {
	conflictTable.lock.Lock()
	defer  conflictTable.lock.Unlock()
	for _, key := range keys {
		value := conflictTable.get(key)
		if value == nil {
			return conflictTable.put(key)
		} else {
			return value
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
		waitList: make(map[string]chan(struct{})),
	}
}
