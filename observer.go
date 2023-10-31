package sm

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type ObserverClient struct {
	kvClient KVWatcher

	state *observerState
}

func NewObserverClient(
	kvClient KVWatcher,
	ns string, // namespace
) (*ObserverClient, error) {
	c := &ObserverClient{
		kvClient: kvClient,
	}

	keyPrefix := computeNamespacePrefix(ns)

	resp, err := c.kvClient.Get(context.Background(),
		keyPrefix,
		clientv3.WithPrefix(),
		clientv3.WithSerializable(),
	)
	if err != nil {
		return nil, err
	}

	c.state, err = newObserverState(ns, resp.Kvs)
	if err != nil {
		return nil, err
	}

	watchChan := c.kvClient.Watch(context.Background(), keyPrefix, clientv3.WithPrefix())
	go c.handleWatchChan(watchChan)

	return c, nil
}

func (c *ObserverClient) handleWatchChan(watchChan clientv3.WatchChan) {
	for resp := range watchChan {
		fmt.Println(resp.Events)
	}
}

// =========================
// Observer Session
// =========================

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
