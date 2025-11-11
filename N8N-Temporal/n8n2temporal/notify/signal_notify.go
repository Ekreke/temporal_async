package notify

// SignalMetadata 信号元数据，输入节点传入，输出节点将带回
type SignalMetadata struct {
	Branch     string `json:"branch"`      // 当前节点分支
	NextBranch string `json:"next_branch"` // 下个节点分支
	TraceId    string `json:"trace_id"`    // 链路ID
	Label      string `json:"label"`       // 自定义标签
}

// SignalInput 信号输入
type SignalInput struct {
	NodeName string `json:"node_name"` // 当前节点ID
	//Data     map[string]interface{} `json:"data"`      // 当前节点执行响应数据
	Data     []map[string]interface{} `json:"data"`     // 当前节点的执行影响数据，一个节点的响应可能是单个值，也可能是多个值，但都包裹在数组中，便于统一处理
	Metadata SignalMetadata           `json:"metadata"` // 元数据，透传
}

// SignalOutput 信号输出（发送到调度）
type SignalOutput struct {
	Metadata SignalMetadata `json:"metadata"`  // 元数据，透传
	NodeName string         `json:"node_name"` // 当前节点ID
}
