package sm

import (
	"fmt"
	"strconv"
	"strings"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

func computeNamespacePrefix(ns string) string {
	return "/" + ns + "/"
}

type observerState struct {
	ns      string
	members map[MemberID]MemberInfo
	shards  []ShardInfo
}

func (s *observerState) cloneMembers() map[MemberID]MemberInfo {
	result := map[MemberID]MemberInfo{}
	for k, v := range s.members {
		result[k] = v
	}
	return result
}

func (s *observerState) cloneShards() []ShardInfo {
	result := make([]ShardInfo, len(s.shards))
	copy(result, s.shards)
	return result
}

func findNumShards(ns string, events []*mvccpb.Event) (uint32, error) {
	numShardsKey := ns + NumShardsKey
	for _, ev := range events {
		if ev.Type != mvccpb.PUT {
			continue
		}
		kv := ev.Kv

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

type doOnce struct {
	done bool
	fn   func()
}

func (o *doOnce) doFunc() {
	if o.done {
		return
	}
	o.fn()
	o.done = true
}

func newObserverState(namespace string, kvs []*mvccpb.KeyValue) (*observerState, error) {
	s := &observerState{
		ns: computeNamespacePrefix(namespace),
	}

	events := make([]*mvccpb.Event, 0, len(kvs))
	for _, kv := range kvs {
		events = append(events, &mvccpb.Event{
			Type: mvccpb.PUT,
			Kv:   kv,
		})
	}

	if err := s.handleEventsInternal(events); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *observerState) handleEventsInternal(events []*mvccpb.Event) error {
	ns := s.ns

	cloneMembersOnce := doOnce{fn: func() { s.members = s.cloneMembers() }}
	cloneShardsOnce := doOnce{fn: func() { s.shards = s.cloneShards() }}

	numShards, err := findNumShards(ns, events)
	if err != nil {
		return err
	}

	oldLen := uint32(len(s.shards))
	if numShards > oldLen {
		cloneShardsOnce.doFunc()

		for i := oldLen; i < numShards; i++ {
			s.shards = append(s.shards, ShardInfo{
				ID: ShardID(oldLen + i),
			})
		}
	}

	const memberPrefix = MemberKeyPrefix + "/"
	const shardPrefix = ShardKeyPrefix + "/"

	for _, ev := range events {
		if ev.Type != mvccpb.PUT {
			panic("TODO")
		}

		kv := ev.Kv
		key := strings.TrimPrefix(string(kv.Key), ns)

		if strings.HasPrefix(key, memberPrefix) {
			memberID := strings.TrimPrefix(key, memberPrefix)

			info, err := UnmarshalMemberInfo(kv.Value)
			if err != nil {
				return fmt.Errorf("sm: unmarshal member data, err: %w", err)
			}

			cloneMembersOnce.doFunc()
			s.members[MemberID(memberID)] = info
			continue
		}

		if strings.HasPrefix(key, shardPrefix) {
			shardIDStr := strings.TrimPrefix(key, shardPrefix)
			shardID, err := strconv.ParseUint(shardIDStr, 10, 32)
			if err != nil {
				return fmt.Errorf("sm: parse shard id, err: %w", err)
			}

			info, err := UnmarshalShardInfo(kv.Value)
			if err != nil {
				return fmt.Errorf("sm: unmarshal shard data, err: %w", err)
			}

			cloneShardsOnce.doFunc()
			info.ID = ShardID(shardID)
			s.shards[shardID] = info
			continue
		}
	}

	return nil
}

func (s *observerState) handleEvents(events []*mvccpb.Event) (*observerState, error) {
	newState := &observerState{
		ns:      s.ns,
		members: s.members,
		shards:  s.shards,
	}
	return newState, newState.handleEventsInternal(events)
}
