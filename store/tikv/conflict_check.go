package tikv

import (
	"github.com/zond/gotomic"
	"github.com/pingcap/tidb/util/lock"
	log "github.com/Sirupsen/logrus"
	"sort"
)

type conflictCheckTable struct {
	hashTable *gotomic.Hash
	//hashTable map[string]interface{}
	//hashTable map[string] *chan struct{}
	//lock      sync.Mutex
}

var conflictTable *conflictCheckTable

func(c *conflictCheckTable) put(key string) *lock.WaitLock {
	log.Infof("[XUWT] put key(%s)", string(key))
	putLock := lock.NewWaitLock()
	if c.hashTable.PutIfMissing(gotomic.StringKey(key), putLock) {
		return putLock
	} else {
		value, ok := c.hashTable.Get(gotomic.StringKey(key))
		if ok {
			waitLock := value.(*lock.WaitLock)
			return waitLock
		}
	}

	return nil
}

func(c *conflictCheckTable) delete (key []byte) {
	//delete(c.hashTable, string(key))
	//log.Infof("[XUWT] delete key(%s)", string(key))
}

func(c *conflictCheckTable) get(key string) *lock.WaitLock {
	log.Infof("[XUWT] check key(%s)", string(key))
	value, ok := c.hashTable.Get(gotomic.StringKey(key))
	if ok {
		log.Infof("[XUWT] get key(%s)", string(key))
		lock := value.(*lock.WaitLock)
		return lock
	} else {
		return nil
	}
}

func checkConflict(keys [][]byte) []*lock.WaitLock  {
	//conflictTable.lock.Lock()
	//defer conflictTable.lock.Unlock()
	lockArray := []*lock.WaitLock{}
	sortKeys := []string{}
	for _, key := range keys {
		sortKeys = append(sortKeys, string(key))
	}
	sort.Strings(sortKeys)

	for _, key := range sortKeys {
		lock := conflictTable.get(key)
		if lock == nil {
			lock = conflictTable.put(key)
		}
		lockArray = append(lockArray, lock)
	}
	return lockArray
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
