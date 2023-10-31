package sm

import (
	"sync"
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

func TestObserverState_New(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		s, err := newObserverState("sample", nil)
		assert.Equal(t, nil, err)

		assert.Equal(t, map[MemberID]MemberInfo(nil), s.members)
		assert.Equal(t, 0, len(s.shards))
	})

	t.Run("with num shards", func(t *testing.T) {
		s, err := newObserverState("sample", []*mvccpb.KeyValue{
			newKV("/sample/num_shards", "3"),
		})
		assert.Equal(t, nil, err)

		assert.Equal(t, map[MemberID]MemberInfo(nil), s.members)
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
			newMemberInfoKV("/sample/members/21", MemberInfo{
				ID:   "21",
				Addr: "addr01",
			}),
			newMemberInfoKV("/sample/members/22", MemberInfo{
				ID:   "22",
				Addr: "addr02",
			}),
			newKV("/sample/num_shards", "3"),
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
			newMemberInfoKV("/sample/members/21", MemberInfo{
				ID:   "21",
				Addr: "addr01",
			}),
			newShardInfoKV("/sample/shards/1", ShardInfo{
				ID:       1,
				Status:   ShardStatusActive,
				Owner:    "21",
				Revision: 11,
			}),
			newKV("/sample/num_shards", "3"),
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

func TestObserverState_HandleEvents(t *testing.T) {
	t.Run("handle num shards from empty", func(t *testing.T) {
		s, err := newObserverState("sample", nil)
		assert.Equal(t, nil, err)

		newState, err := s.handleEvents([]*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv:   newKV("/sample/num_shards", "4"),
			},
		})
		assert.Equal(t, nil, err)

		assert.Equal(t, 0, len(s.shards))
		assert.Equal(t, 4, len(newState.shards))

		assert.Equal(t, s.members, newState.members)
		assert.NotSame(t, s.shards, newState.shards)
	})

	t.Run("handle num shards from existing", func(t *testing.T) {
		s, err := newObserverState("sample", []*mvccpb.KeyValue{
			newKV("/sample/num_shards", "3"),
		})
		assert.Equal(t, nil, err)

		newState, err := s.handleEvents([]*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv:   newKV("/sample/num_shards", "4"),
			},
		})
		assert.Equal(t, nil, err)

		assert.Equal(t, 3, len(s.shards))
		assert.Equal(t, 4, len(newState.shards))

		assert.Equal(t, s.members, newState.members)
		assert.NotSame(t, s.shards, newState.shards)
	})

	t.Run("handle shard change owner", func(t *testing.T) {
		s, err := newObserverState("sample", []*mvccpb.KeyValue{
			newShardInfoKV("/sample/shards/2", ShardInfo{
				ID:       2,
				Status:   ShardStatusActive,
				Owner:    "21",
				Revision: 31,
			}),
			newKV("/sample/num_shards", "4"),
		})
		assert.Equal(t, nil, err)

		newState, err := s.handleEvents([]*mvccpb.Event{
			{
				Type: mvccpb.PUT,
				Kv: newShardInfoKV("/sample/shards/2", ShardInfo{
					ID:       2,
					Status:   ShardStatusActive,
					Owner:    "22",
					Revision: 32,
				}),
			},
		})
		assert.Equal(t, nil, err)

		assert.Equal(t, 4, len(s.shards))
		assert.Equal(t, 4, len(newState.shards))

		assert.Equal(t, s.members, newState.members)
		assert.NotSame(t, s.shards, newState.shards)

		assert.Equal(t, ShardInfo{
			ID:       2,
			Status:   ShardStatusActive,
			Owner:    "21",
			Revision: 31,
		}, s.shards[2])

		assert.Equal(t, ShardInfo{
			ID:       2,
			Status:   ShardStatusActive,
			Owner:    "22",
			Revision: 32,
		}, newState.shards[2])
	})
}

func handleConcurrentTest(initFn func() *observerState, handleFn func(s *observerState)) {
	for i := 0; i < 100; i++ {
		state := initFn()

		var wg sync.WaitGroup
		for th := 0; th < 3; th++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				handleFn(state)
			}()
		}
	}
}

func TestObserverState_HandleEvents_Concurrent(t *testing.T) {
	t.Run("add new member from existing", func(t *testing.T) {
		handleConcurrentTest(func() *observerState {
			s, err := newObserverState("sample", []*mvccpb.KeyValue{
				newMemberInfoKV("/sample/members/21", MemberInfo{
					ID:   "21",
					Addr: "addr01",
				}),
			})
			assert.Equal(t, nil, err)
			return s
		}, func(s *observerState) {
			_, err := s.handleEvents([]*mvccpb.Event{
				{
					Type: mvccpb.PUT,
					Kv: newMemberInfoKV("/sample/members/22", MemberInfo{
						ID:   "22",
						Addr: "addr02",
					}),
				},
			})
			assert.Equal(t, nil, err)
		})
	})
}
