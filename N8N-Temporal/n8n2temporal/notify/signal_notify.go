package notify

import "errors"

// SignalMetadata 信号元数据，输入节点传入，输出节点将带回
type SignalMetadata struct {
	Branch     string `json:"branch"`      // 当前节点分支
	NextBranch string `json:"next_branch"` // 下个节点分支
	TraceId    string `json:"trace_id"`    // 链路ID
	Label      string `json:"label"`       // 自定义标签
}

// SignalData 信号输入
type SignalData struct {
	ActivityExecId string                   `json:"activity_exec_id"` // 执行ID，全局唯一，同一个节点被多次执行，每次这个ID都不一样。
	NodeName       string                   `json:"node_name"`        // 当前节点名称
	Data           []map[string]interface{} `json:"data"`             // 当前节点的执行影响数据，一个节点的响应可能是单个值，也可能是多个值，但都包裹在数组中，便于统一处理
	DataId         []string                 `json:"data_id"`          // 数据ID，用于标识每个data成员的唯一ID。用于任务结果信号发送给workflow，标识已执行任务列表
	Metadata       SignalMetadata           `json:"metadata"`         // 元数据，透传
}

// Validate 验证数据安全
func (si SignalData) Validate() error {
	if si.ActivityExecId == "" {
		return errors.New("missing exec_id")
	}
	if si.NodeName == "" {
		return errors.New("missing node_name")
	}
	return nil
}
