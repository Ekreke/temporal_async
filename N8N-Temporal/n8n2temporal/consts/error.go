package consts

import "errors"

type WorkFlowErr error

var (
	WorkFlowNotFound   WorkFlowErr = errors.New("node not found")    // 未找到对应节点
	WorkFlowConnExcept WorkFlowErr = errors.New("connection except") // 连接节点异常
)
