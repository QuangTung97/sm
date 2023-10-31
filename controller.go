package sm

import (
	"context"
	"fmt"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

// ControllerClient ...
type ControllerClient struct {
	client *clientv3.Client
	ns     string

	closeCh    chan struct{}
	memberChan chan struct{}
}

type MemberWatcher interface {
	GetMembers() map[MemberID]MemberInfo
	Wait() (closed bool)
}

func NewControllerClient(
	client *clientv3.Client,
	watcher MemberWatcher,
	ns string,
) *ControllerClient {
	c := &ControllerClient{
		client:     client,
		ns:         computeNamespacePrefix(ns),
		closeCh:    make(chan struct{}),
		memberChan: make(chan struct{}),
	}

	go func() {
		c.handleWatcher(watcher)
	}()

	return c
}

func (c *ControllerClient) handleWatcher(watcher MemberWatcher) {
	for {
		closed := watcher.Wait()
		if closed {
			close(c.closeCh)
			return
		}

		c.memberChan <- struct{}{}
	}
}

type controllerLeaderState struct {
	client *clientv3.Client
	ns     string

	election *concurrency.Election
}

func (c *ControllerClient) Run() (closed bool, err error) {
	sess, err := concurrency.NewSession(c.client)
	if err != nil {
		return false, fmt.Errorf("sm: new session, err: %w", err)
	}

	election := concurrency.NewElection(sess, c.ns+"elections/")

	err = election.Campaign(context.Background(), "{}")
	if err != nil {
		return false, err
	}

	state := &controllerLeaderState{
		client: c.client,
		ns:     c.ns,

		election: election,
	}

	if err := state.handleMembersChanged(); err != nil {
		return false, err
	}

	for {
		select {
		case <-c.memberChan:
			if err := state.handleMembersChanged(); err != nil {
				return false, err
			}

		case <-sess.Done():
			return false, nil

		case <-c.closeCh:
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			_ = election.Resign(ctx)
			cancel()
			return true, nil
		}
	}
}

func (s *controllerLeaderState) handleMembersChanged() error {
	resp, err := s.client.Get(
		context.Background(),
		computeNamespacePrefix(s.ns),
		clientv3.WithPrefix(),
	)
	if err != nil {
		return err
	}

	state, err := newObserverState(s.ns, resp.Kvs)
	if err != nil {
		return err
	}

	fmt.Println(state)

	return nil
}
