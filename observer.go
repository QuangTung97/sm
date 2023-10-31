package sm

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type observerState struct {
	members map[MemberID]MemberInfo
	shards  []ShardInfo
}

func findNumShards(ns string, kvs []*mvccpb.KeyValue) (uint32, error) {
	numShardsKey := ns + "/" + NumShardsKey
	for _, kv := range kvs {
		if string(kv.Key) == numShardsKey {
			num, err := strconv.ParseUint(string(kv.Value), 10, 32)
			if err != nil {
				return 0, fmt.Errorf("sm: find num shards, err: %w", err)
			}
			return uint32(num), nil
		}
	}
	return 0, nil
}

func newObserverState(ns string, kvs []*mvccpb.KeyValue) (*observerState, error) {
	numShards, err := findNumShards(ns, kvs)
	if err != nil {
		return nil, err
	}

	shards := make([]ShardInfo, numShards)
	for i := range shards {
		shards[i].ID = ShardID(i)
	}

	ns = ns + "/"

	const memberPrefix = MemberKeyPrefix + "/"
	const shardPrefix = ShardKeyPrefix + "/"

	members := map[MemberID]MemberInfo{}

	for _, kv := range kvs {
		key := strings.TrimPrefix(string(kv.Key), ns)

		if strings.HasPrefix(key, memberPrefix) {
			memberID := strings.TrimPrefix(key, memberPrefix)

			info, err := UnmarshalMemberInfo(kv.Value)
			if err != nil {
				return nil, fmt.Errorf("sm: unmarshal member data, err: %w", err)
			}

			members[MemberID(memberID)] = info
			continue
		}

		if strings.HasPrefix(key, shardPrefix) {
			shardIDStr := strings.TrimPrefix(key, shardPrefix)
			shardID, err := strconv.ParseUint(shardIDStr, 10, 32)
			if err != nil {
				return nil, fmt.Errorf("sm: parse shard id, err: %w", err)
			}

			info, err := UnmarshalShardInfo(kv.Value)
			if err != nil {
				return nil, fmt.Errorf("sm: unmarshal shard data, err: %w", err)
			}

			info.ID = ShardID(shardID)
			shards[shardID] = info
			continue
		}
	}

	return &observerState{
		members: members,
		shards:  shards,
	}, nil
}

func (s *observerState) handleEvents(events []*mvccpb.Event) {
}

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

	keyPrefix := ns + "/"

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
