package node

var GlobalNode = make(map[string]Activity)

func RegisterNode(node Activity) {
	nodeInfo := node.GetNodeInfo()
	GlobalNode[nodeInfo.Name] = node
}
