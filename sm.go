package sm

// ShardID is a value in range [0, (numShards - 1)]
type ShardID uint32

// MemberID is an uuid
type MemberID string

const (
	MemberKeyPrefix = "members"
	ShardKeyPrefix  = "shards"
)

type MemberInfo struct {
	ID   MemberID
	Addr string

	Extra ExtraMemberInfo
}

type ShardStatus int

const (
	ShardStatusUnassigned ShardStatus = 0
	ShardStatusActive     ShardStatus = 1
)

type ShardRevision uint64

type ShardInfo struct {
	ID       ShardID
	Status   ShardStatus
	Owner    MemberID
	Revision ShardRevision

	Extra ExtraShardInfo
}

// =================================
// Extra Infos
// =================================

type ExtraMemberInfo struct {
	PrepareToLeave bool
}

type ExtraShardInfo struct {
	PrepareToDrop bool
}
