package sm

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMarshalMemberInfo(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		info := MemberInfo{
			ID:   "21",
			Addr: "addr01:2000",
		}
		data, err := info.Marshal()
		assert.Equal(t, nil, err)
		assert.Equal(t, `{"id":"21","addr":"addr01:2000"}`, string(data))

		result, err := UnmarshalMemberInfo(data)
		assert.Equal(t, nil, err)
		assert.Equal(t, info, result)
	})
}

func formatJSON(s string) string {
	data := json.RawMessage(s)
	result, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		panic(err)
	}
	return string(result)
}

func TestMarshalShardInfo(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		info := ShardInfo{
			ID:       5,
			Status:   ShardStatusActive,
			Owner:    "21",
			Revision: 11,
			Extra: ExtraShardInfo{
				PrepareToDrop: true,
			},
		}

		data, err := info.Marshal()
		assert.Equal(t, nil, err)

		assert.Equal(t, strings.TrimSpace(`
{
  "id": 5,
  "status": 1,
  "owner": "21",
  "revision": 11,
  "prepare_to_drop": true
}
`), formatJSON(string(data)))

		result, err := UnmarshalShardInfo(data)
		assert.Equal(t, nil, err)
		assert.Equal(t, info, result)
	})
}
