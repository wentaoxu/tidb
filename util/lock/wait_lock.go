package lock

import "context"

type WaitLock struct {
	lock chan struct{}
}

func NewWaitLock() *WaitLock {
	return &WaitLock{make(chan struct{}, 1)}
}

func (wl *WaitLock) Lock(ctx context.Context) bool {
	select {
	case wl.lock<-struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (wl *WaitLock) UnLock() {
	<-wl.lock
}

func (wl *WaitLock) TryLock() bool {
	select {
	case wl.lock<- struct{}{}:
		return true
	default:
		return false
	}
}

func (wl *WaitLock) TryUnLock() bool {
	select {
	case <-wl.lock:
		return true
	default:
		return false
	}
}
