package sm

import (
	"encoding/json"
)

type memberInfoData struct {
	ID   MemberID `json:"id"`
	Addr string   `json:"addr"`
}

func (i MemberInfo) Marshal() ([]byte, error) {
	return json.Marshal(memberInfoData{
		ID:   i.ID,
		Addr: i.Addr,
	})
}

func UnmarshalMemberInfo(data []byte) (MemberInfo, error) {
	var info memberInfoData
	err := json.Unmarshal(data, &info)
	if err != nil {
		return MemberInfo{}, err
	}
	return MemberInfo{
		ID:   info.ID,
		Addr: info.Addr,
	}, nil
}

type shardInfoData struct {
	ID       ShardID       `json:"id"`
	Status   ShardStatus   `json:"status"`
	Owner    MemberID      `json:"owner"`
	Revision ShardRevision `json:"revision"`

	PrepareToDrop bool `json:"prepare_to_drop,omitempty"`
}

func (i ShardInfo) Marshal() ([]byte, error) {
	return json.Marshal(shardInfoData{
		ID:       i.ID,
		Status:   i.Status,
		Owner:    i.Owner,
		Revision: i.Revision,

		PrepareToDrop: i.Extra.PrepareToDrop,
	})
}

func UnmarshalShardInfo(data []byte) (ShardInfo, error) {
	var info shardInfoData
	err := json.Unmarshal(data, &info)
	if err != nil {
		return ShardInfo{}, err
	}
	return ShardInfo{
		ID:       info.ID,
		Status:   info.Status,
		Owner:    info.Owner,
		Revision: info.Revision,

		Extra: ExtraShardInfo{
			PrepareToDrop: info.PrepareToDrop,
		},
	}, nil
}
