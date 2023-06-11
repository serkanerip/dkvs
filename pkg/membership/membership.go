package membership

import (
	"dkvs/pkg/message"
	"dkvs/pkg/node"
	"fmt"
)

type MemberShip struct {
	Nodes    []*node.Node
	LeaderID string
}

type UpdatedEvent struct {
	Membership *MemberShip `msgpack:"membership"`
}

func (p *UpdatedEvent) Type() message.MsgType {
	return message.MembershipUpdatedE
}

func (m *MemberShip) NodeIds() []string {
	var ids []string
	for i := range m.Nodes {
		ids = append(ids, m.Nodes[i].ID)
	}
	return ids
}

func (m *MemberShip) PrettyPrint() {
	output := "Membership View[ "
	for i := range m.Nodes {
		output += m.Nodes[i].ID
		if i != len(m.Nodes)-1 {
			output += ","
		}
	}
	output += " ]"
	fmt.Println(output)
}
