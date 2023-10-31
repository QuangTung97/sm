package sm

type MemberClient struct {
}

func NewMemberClient() *MemberClient {
	return &MemberClient{}
}

func (c *MemberClient) GetMemberID() MemberID {
	return ""
}

func (c *MemberClient) Run() {
}
