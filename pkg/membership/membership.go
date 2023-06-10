package membership

import (
	"dkvs/pkg/node"
)

type MemberShip struct {
	Nodes    []*node.Node
	LeaderID string
}
