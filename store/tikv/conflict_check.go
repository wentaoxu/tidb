package tikv

import (
	"github.com/zond/gotomic"
	//"sync"
	//log "github.com/Sirupsen/logrus"
	//"github.com/pingcap/tidb/kv"
	log "github.com/Sirupsen/logrus"
)

type conflictCheckTable struct {
	hashTable *gotomic.Hash
	//hashTable map[string]interface{}
	//hashTable map[string] *chan struct{}
	//lock      sync.Mutex
}

var conflictTable *conflictCheckTable

func(c *conflictCheckTable) put(key []byte) *chan struct{} {
	log.Infof("[XUWT] put key(%s)", string(key))
	lock := make(chan struct{}, 1)
	if c.hashTable.PutIfMissing(gotomic.StringKey(key), &lock) {
		return &lock
	} else {
		value, ok := c.hashTable.Get(gotomic.StringKey(key))
		if ok {
			lock := value.(*chan struct{})
			return lock
		}
	}

	return nil
}

func(c *conflictCheckTable) delete (key []byte) {
	//delete(c.hashTable, string(key))
	//log.Infof("[XUWT] delete key(%s)", string(key))
}

func(c *conflictCheckTable) get(key []byte) *chan struct{} {
	log.Infof("[XUWT] check key(%s)", string(key))
	value, ok := c.hashTable.Get(gotomic.StringKey(key))
	if ok {
		log.Infof("[XUWT] get key(%s)", string(key))
		lock := value.(*chan struct{})
		return lock
	} else {
		return nil
	}
}

func checkConflict(keys [][]byte) *chan struct{}  {
	//conflictTable.lock.Lock()
	//defer conflictTable.lock.Unlock()
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
	//conflictTable.lock.Lock()
	//defer conflictTable.lock.Unlock()
	for _, key := range keys {
		conflictTable.delete(key)
	}
}

func init() {
	conflictTable = &conflictCheckTable{
		hashTable: gotomic.NewHash(),
	}
}
