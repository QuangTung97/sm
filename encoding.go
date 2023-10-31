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
