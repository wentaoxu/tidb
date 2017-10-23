package tikv

import (
	log "github.com/Sirupsen/logrus"
	//"github.com/zond/gotomic"
	"sync"
	"github.com/pingcap/tidb/kv"
)

type conflictCheckTable struct {
	//hashTable *gotomic.Hash
	hashTable map[string]interface{}
	lock sync.Mutex
}

var conflictTable *conflictCheckTable

func(c *conflictCheckTable) put(key []byte, value interface{}) {
	c.hashTable[string(key)] = value
	//log.Infof("[XUWT] put key(%s)", string(key))
}

func(c *conflictCheckTable) delete (key []byte) {
	delete(c.hashTable, string(key))
	//log.Infof("[XUWT] delete key(%s)", string(key))
}

func(c *conflictCheckTable) get(key []byte) interface {} {
	value, ok := c.hashTable[string(key)]
	if ok {
		//log.Infof("[XUWT] get key(%s)", string(key))
		return value
	} else {
		//log.Infof("[XUWT] not to get key(%s)", string(key))
		return nil
	}
}

func checkConflict(keys [][]byte, txn *tikvTxn) (chan(struct{}), error) {
	conflictTable.lock.Lock()
	defer  conflictTable.lock.Unlock()
	for _, key := range keys {
		value := conflictTable.get(key)
		if value != nil {
			//log.Infof("[XUWT] txn(%d) get conflicted key(%s)", txn.startTS, string(key))
			conflictTxn, ok := value.(*tikvTxn)
			if ok {
				//log.Infof("[XUWT] txn(%d) get conflicted txn(%d) with status(%d)", txn.startTS, conflictTxn.startTS, conflictTxn.status)
				if conflictTxn.status == txnOnGoing {
					return conflictTxn.blocked, kv.ErrLockConflict
				} else if conflictTxn.commitTS > txn.startTS {
					return nil, kv.ErrLockConflict
				}
			} else {
				log.Fatal("can get txn")
			}
		}
	}
	for _, key := range keys {
		conflictTable.put(key, txn)
	}
	return nil, nil
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
		hashTable: make(map[string]interface{}),
	}
}
