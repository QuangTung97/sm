package sm

import (
	clientv3 "go.etcd.io/etcd/client/v3"
)

// ControllerClient ...
type ControllerClient struct {
	kvClient clientv3.KV
	watcher  MemberWatcher
}

type MemberWatcher interface {
	GetMembers() map[MemberID]MemberInfo
	Wait() (closed bool)
}

func NewControllerClient(
	kvClient clientv3.KV,
	watcher MemberWatcher,
) *ControllerClient {
	return &ControllerClient{
		kvClient: kvClient,
		watcher:  watcher,
	}
}

func (c *ControllerClient) Run() {
}
