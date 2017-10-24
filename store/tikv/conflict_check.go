package tikv

import (
	"github.com/zond/gotomic"
	"github.com/pingcap/tidb/util/lock"
	log "github.com/Sirupsen/logrus"
	"sort"
	"github.com/pingcap/tidb/tablecodec"
	fnv "hash/fnv"
)

type conflictCheckTable struct {
	hashTable *gotomic.Hash
	//hashTable map[string]interface{}
	//hashTable map[string] *chan struct{}
	//lock      sync.Mutex
}

var conflictTable *conflictCheckTable

func(c *conflictCheckTable) put(key int) *lock.WaitLock {
	//log.Infof("[XUWT] put key(%d)", key)
	putLock := lock.NewWaitLock()
	if c.hashTable.PutIfMissing(gotomic.IntKey(key), putLock) {
		return putLock
	} else {
		value, ok := c.hashTable.Get(gotomic.IntKey(key))
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

func(c *conflictCheckTable) get(key int) *lock.WaitLock {
	//log.Infof("[XUWT] check key(%s)", string(key))
	value, ok := c.hashTable.Get(gotomic.IntKey(key))
	if ok {
		//log.Infof("[XUWT] get key(%s)", string(key))
		lock := value.(*lock.WaitLock)
		return lock
	} else {
		return nil
	}
}

func checkConflict(keys [][]byte) []*lock.WaitLock  {
	//conflictTable.lock.Lock()
	//defer conflictTable.lock.Unlock()
	uniq := make(map[uint32]int)
	for _, key := range keys {
		if tablecodec.IsRecordKey(key) {
			fnv := fnv.New32()
			fnv.Write(key)
			uniq[fnv.Sum32()] = 0
		}
	}

	sortKeys := []int{}
	for key, _ := range uniq {
		sortKeys = append(sortKeys, int(key))
	}
	sort.Ints(sortKeys)

	lockArray := []*lock.WaitLock{}
	for _, key := range sortKeys {
		lock := conflictTable.get(key)
		if lock == nil {
			lock = conflictTable.put(key)
		}
		lockArray = append(lockArray, lock)
	}
	//log.Infof("[XUWT] lock array len %d", len(lockArray))
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
