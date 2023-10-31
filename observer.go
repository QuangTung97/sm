package sm

import (
	clientv3 "go.etcd.io/etcd/client/v3"
)

type ObserverClient struct {
	watchClient clientv3.Watcher
}

func NewObserverClient(
	watchClient clientv3.Watcher,
	ns string, // namespace
) *ObserverClient {
	return &ObserverClient{
		watchClient: watchClient,
	}
}

type ObserverSession struct {
}

func (c *ObserverClient) NewSession() *ObserverSession {
	return &ObserverSession{}
}

func (s *ObserverSession) GetMembers() map[MemberID]MemberInfo {
	return nil
}

func (s *ObserverSession) GetShards() []ShardInfo {
	return nil
}

// Wait for changes
func (s *ObserverSession) Wait() (closed bool) {
	return false
}

func (c *ObserverClient) Close() error {
	return nil
}
