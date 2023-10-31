package sm

import (
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
