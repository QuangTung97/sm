package sm

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func newKV(key string, val string) *mvccpb.KeyValue {
	return &mvccpb.KeyValue{
		Key:   []byte(key),
		Value: []byte(val),
	}
}

func newMemberInfoKV(key string, info MemberInfo) *mvccpb.KeyValue {
	data, err := info.Marshal()
	if err != nil {
		panic(err)
	}
	return newKV(key, string(data))
}

func newShardInfoKV(key string, info ShardInfo) *mvccpb.KeyValue {
	data, err := info.Marshal()
	if err != nil {
		panic(err)
	}
	return newKV(key, string(data))
}

func TestObserverState(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		s, err := newObserverState("sample", nil)
		assert.Equal(t, nil, err)

		assert.Equal(t, map[MemberID]MemberInfo{}, s.members)
		assert.Equal(t, 0, len(s.shards))
	})

	t.Run("with num shards", func(t *testing.T) {
		s, err := newObserverState("sample", []*mvccpb.KeyValue{
			newKV("sample/num_shards", "3"),
		})
		assert.Equal(t, nil, err)

		assert.Equal(t, map[MemberID]MemberInfo{}, s.members)
		assert.Equal(t, []ShardInfo{
			{
				ID:       0,
				Status:   ShardStatusUnassigned,
				Revision: 0,
			},
			{
				ID:       1,
				Status:   ShardStatusUnassigned,
				Revision: 0,
			},
			{
				ID:       2,
				Status:   ShardStatusUnassigned,
				Revision: 0,
			},
		}, s.shards)
	})

	t.Run("with member infos", func(t *testing.T) {
		s, err := newObserverState("sample", []*mvccpb.KeyValue{
			newMemberInfoKV("sample/members/21", MemberInfo{
				ID:   "21",
				Addr: "addr01",
			}),
			newMemberInfoKV("sample/members/22", MemberInfo{
				ID:   "22",
				Addr: "addr02",
			}),
			newKV("sample/num_shards", "3"),
		})
		assert.Equal(t, nil, err)

		assert.Equal(t, map[MemberID]MemberInfo{
			"21": {
				ID:   "21",
				Addr: "addr01",
			},
			"22": {
				ID:   "22",
				Addr: "addr02",
			},
		}, s.members)
	})

	t.Run("with shard info", func(t *testing.T) {
		s, err := newObserverState("sample", []*mvccpb.KeyValue{
			newMemberInfoKV("sample/members/21", MemberInfo{
				ID:   "21",
				Addr: "addr01",
			}),
			newShardInfoKV("sample/shards/1", ShardInfo{
				ID:       1,
				Status:   ShardStatusActive,
				Owner:    "21",
				Revision: 11,
			}),
			newKV("sample/num_shards", "3"),
		})
		assert.Equal(t, nil, err)

		assert.Equal(t, []ShardInfo{
			{
				ID:       0,
				Status:   ShardStatusUnassigned,
				Revision: 0,
			},
			{
				ID:       1,
				Status:   ShardStatusActive,
				Owner:    "21",
				Revision: 11,
			},
			{
				ID:       2,
				Status:   ShardStatusUnassigned,
				Revision: 0,
			},
		}, s.shards)
	})
}
