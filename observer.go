package sm

import (
	"context"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

type ObserverClient struct {
	ns        string
	keyPrefix string
	kvClient  KVWatcher

	watchCtx    context.Context
	cancelWatch func()
	watchChan   clientv3.WatchChan

	state   atomic.Pointer[observerState]
	closeCh chan struct{}
}

func NewObserverClient(
	kvClient KVWatcher, ns string, // namespace
) (*ObserverClient, error) {
	c := &ObserverClient{
		ns:        ns,
		keyPrefix: computeNamespacePrefix(ns),
		kvClient:  kvClient,
	}

	c.watchCtx, c.cancelWatch = context.WithCancel(context.Background())

	if err := c.getStateAndBuildWatchChan(); err != nil {
		return nil, err
	}

	return c, nil
}

func (c *ObserverClient) getStateAndBuildWatchChan() error {
	resp, err := c.kvClient.Get(context.Background(),
		c.keyPrefix,
		clientv3.WithPrefix(),
		clientv3.WithSerializable(),
	)
	if err != nil {
		return err
	}

	state, err := newObserverState(c.ns, resp.Kvs)
	if err != nil {
		return err
	}
	c.state.Store(state)

	c.watchChan = c.kvClient.Watch(
		c.watchCtx, c.keyPrefix,
		clientv3.WithPrefix(),
		clientv3.WithRev(resp.Header.Revision+1),
	)

	return nil
}

func (c *ObserverClient) consumeWatchChan() error {
	for resp := range c.watchChan {
		if resp.Err() != nil {
			return fmt.Errorf("consume watch chan, err: %w", resp.Err())
		}

		obsState := c.state.Load()
		newState, err := obsState.handleEvents(resp.Events)
		if err != nil {
			return fmt.Errorf("consume watch chan, err: %w", err)
		}

		c.state.Store(newState)
	}

	return nil
}

func sleepContext(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}

func (c *ObserverClient) Run() {
	defer close(c.closeCh)

	duration := 10 * time.Second

	for {
		err := c.consumeWatchChan()
		if err == nil {
			return
		}

		log.Println("[ERROR] sm:", err)

		sleepContext(c.watchCtx, duration)
		if c.watchCtx.Err() != nil {
			return
		}

		for {
			err = c.getStateAndBuildWatchChan()
			if err == nil {
				break
			}

			log.Println("[ERROR] sm:", err)

			sleepContext(c.watchCtx, duration)
			if c.watchCtx.Err() != nil {
				return
			}

			continue
		}
	}
}

func (c *ObserverClient) Close() error {
	c.cancelWatch()

	<-c.closeCh
	return nil
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
