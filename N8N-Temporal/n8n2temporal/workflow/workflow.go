package workflow

// 将n8n中的json数据，转换成temporal的执行流程

import (
	"context"
	"errors"
	"fmt"
	"n8n2temporal/consts"
	nodepkg "n8n2temporal/node"
	"n8n2temporal/notify"
	"n8n2temporal/pkg/util"
	"n8n2temporal/workflow/wkGraph"
	"slices"
	"strings"
	"time"

	"github.acme.red/wego/pkg/utils/maputil/v2"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// NodeExecResult 节点执行结果
type NodeExecResult struct {
	NodeID        string                   `json:"node_id"`            // 节点ID
	NodeName      string                   `json:"node_name"`          // 节点名称
	Success       bool                     `json:"success"`            // 执行成功与否
	Data          []map[string]interface{} `json:"data"`               // 执行响应数据(不一定是节点的结果，只能说执行响应)
	Error         string                   `json:"error,omitempty"`    // 错误原因
	Metadata      notify.SignalMetadata    `json:"metadata,omitempty"` // 头数据，用于透传到下一个执行节点
	AddTaskNum    int                      `json:"add_task_num"`       // 新增任务数
	FinishTaskNum int                      `json:"finish_task_num"`    // 完成任务数
}

// executeNode 执行单个节点
func executeNode(ctx workflow.Context, node nodepkg.WkFLowNode, inputData map[string]interface{}, express *nodepkg.ExpressionEvaluator, signalInput *notify.SignalInput) (*NodeExecResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("开始执行节点", "nodeId", node.ID, "nodeName", node.Name, "nodeType", node.Type)
	result := &NodeExecResult{
		NodeID:   node.ID,
		Success:  false,
		Data:     make([]map[string]interface{}, 0),
		Metadata: signalInput.Metadata,
	}

	// 设置活动选项
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour * 1,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)

	// 根据节点类型选择相应的活动
	var err error                               // 节点执行错误
	var nodeResult = &nodepkg.ActivityOutput{}  // 节点执行结果
	var execNode interface{}                    // 执行节点
	var wkInfo = workflow.GetInfo(ctx)          // 获取工作流info信息
	var activityInput = &nodepkg.ActivityInput{ // 节点输入参数
		UniqueId:    util.UUID(), // todo 这里可能会导致不能重放，需要考虑
		NodeID:      node.ID,
		NodeName:    node.Name,
		NodeType:    node.Type,
		InputData:   inputData,
		Parameters:  node.Parameters,
		SignalInput: signalInput,
		WorkflowID:  wkInfo.WorkflowExecution.ID,
		ExecutionID: wkInfo.WorkflowExecution.RunID,
	}
	switch {
	// 开始节点
	case strings.HasSuffix(node.Type, ".start") || strings.HasSuffix(node.Type, ".manualTrigger"):
		execNode = ExecuteStartNode

	// 变量节点
	case strings.HasSuffix(node.Type, ".variable"):
		execNode = ExecuteVariableNode

	// Python代码执行节点
	case strings.HasSuffix(node.Type, ".pythonDocker") || strings.HasSuffix(node.Type, ".code"):
		// Python 代码执行节点 - 使用Python Docker节点
		execNode = ExecutePythonDockerNode

	// 条件判断节点
	case strings.HasSuffix(node.Type, ".condition") || strings.HasSuffix(node.Type, ".if") || strings.HasSuffix(node.Type, ".conditional"):
		// IF 条件节点 - 使用统一条件节点
		execNode = ExecuteConditionalNode

	// 域名解析节点
	case strings.HasSuffix(node.Type, ".domainResolve") || strings.HasSuffix(node.Type, ".asmDomainResolve") || strings.HasSuffix(node.Type, ".asm_domain_resolve"):
		execNode = "RegisterDomainResolve"

	// 结束节点
	case strings.HasSuffix(node.Type, ".end"):
		execNode = ExecuteEndNode

	default:
		logger.Error("不支持的节点类型", "nodeType", node.Type, "nodeName", node.Name, "nodeId", node.ID)
	}
	// 执行实际节点
	if execNode != nil {
		// TODO: 风险提示：将 express（含 WorkflowContext）作为活动入参可能导致 payload 过大，
		//       且为“上下文快照”语义，易与“最新状态”预期不一致。考虑在工作流侧先完成表达式求值，仅传递必要数据。
		err = workflow.ExecuteActivity(ctx, execNode, activityInput, express, node).Get(ctx, &nodeResult)
	} else {
		err = fmt.Errorf("未定义节点类型: %s (节点名称: %s)", node.Type, node.Name)
	}
	// 统一处理响应
	if err != nil {
		result.Error = err.Error()
		logger.Error("节点执行失败", "nodeId", node.ID, "error", err)
	} else {
		result.Success = nodeResult.Success
		result.Data = nodeResult.Data
		result.AddTaskNum = nodeResult.AddTaskNum
		result.FinishTaskNum = nodeResult.FinishTaskNum
		logger.Info("节点执行成功", "nodeId", node.ID)
	}

	// 节点执行完毕后，更新元数据
	result.Metadata.Branch = result.Metadata.NextBranch
	result.Metadata.NextBranch = "main"
	// 只有condition节点的nextBranch需要修改，其他都一定是默认main
	if strings.Contains(nodeResult.NodeType, ".conditional") && len(nodeResult.Data) > 0 {
		matchBranch := maputil.GetMapValue(nodeResult.Data[0], "outputPaths", nil)
		if branch, ok := matchBranch.([]interface{}); ok && len(branch) > 0 {
			nextBranch, ok := branch[0].(string)
			if ok && nextBranch != "" {
				result.Metadata.NextBranch = nextBranch
			}
		}
	}
	return result, err
}

// GenericWorkflowWithMaxStep 是带最大步数限制的通用 Temporal 工作流定义
func GenericWorkflowWithMaxStep(ctx workflow.Context, workflowJson string, initialData map[string]interface{}, maxStep int64) (map[string]interface{}, error) {
	var (
		totalTask int64                                 // todo 待完成停止条件
		stepCount int64                                 // 全局节点执行数量定义 todo 这里Add可能有并发问题
		express   = nodepkg.NewExpressionEvaluator(nil) // 初始化全局变量对象

		execNodeFinalResults = make(map[string]*NodeExecResult)     // 获取每个节点最后一次执行结果(同一节点可能被执行多次，这里记录该节点最后一次执行的结果)
		execNodeCnt          = make(map[string]int)                 // 记录每个节点的执行次数 todo 当前还未使用，后续需在节点完成后累加
		signalChan           = map[string]workflow.ReceiveChannel{} // 信号通道映射,用于监听
		localChan            = map[string]workflow.Channel{}        // 本地通道映射，用于本地发送信号
	)

	// 工作流图解析
	if initialData == nil {
		return nil, errors.New("初始化参数不能为空")
	}
	logger := workflow.GetLogger(ctx)
	logger.Info("开始执行通用 n8n 工作流", "maxStep", maxStep)
	// 创建工作流图对象
	graph, err := wkGraph.NewWkFlowGraph(workflowJson)
	if err != nil {
		return nil, fmt.Errorf("解析工作流定义失败: %v", err)
	}
	if graph.StartNode.Check() != nil || graph.EndNode.Check() != nil {
		return nil, errors.New("缺少开始或结束节点")
	}
	// 检测工作流环
	rings := graph.DetectRings()
	if rings.HasRings && maxStep == 0 {
		return nil, fmt.Errorf("工作流中存在环，但是没有设置最大步数")
	}
	if !rings.ValidateRings() {
		return nil, fmt.Errorf("工作流中存在没有出口的环")
	}
	logger.Info("工作流图解析完成", "nodeCount", len(graph.GetAllNodes()))

	// 创建可取消的上下文用于协程管理
	childCtx, cancelFunc := workflow.WithCancel(ctx)
	defer cancelFunc()
	// 信号通道初始化
	for _, flowNode := range graph.GetAllNodes() {
		if flowNode.IsRemote {
			signalChan[flowNode.Name] = workflow.GetSignalChannel(childCtx, flowNode.Name)
			logger.Info("signalChan", "signalChan_node", flowNode.Name)
		} else {
			localCh := workflow.NewNamedBufferedChannel(childCtx, flowNode.Name, consts.DefaultChannelSize)
			signalChan[flowNode.Name] = localCh
			localChan[flowNode.Name] = localCh
			logger.Info("localChan", "node", flowNode.Name)
		}
	}
	// 对signalChan进行排序，确保后续访问遍历的顺序
	var signalArr = make([]string, 0, len(signalChan))
	for nodeName := range signalChan {
		signalArr = append(signalArr, nodeName)
	}
	slices.Sort(signalArr)
	// 异步信号监听执行
	workflow.Go(childCtx, func(gCtx workflow.Context) {
		selector := workflow.NewSelector(gCtx)
		// 开启全局监听
		for _, signalName := range signalArr {
			nodeName := signalName
			selector.AddReceive(signalChan[nodeName], func(c workflow.ReceiveChannel, more bool) {
				// more标识信号通道是否关闭
				if !more {
					logger.Info("信号通道已关闭", "nodeName", nodeName)
					return
				}
				// 获取信号相关信息
				var signalInput *notify.SignalInput
				c.Receive(gCtx, &signalInput)
				if signalInput == nil {
					logger.Error("信号数据解析失败")
					return
				}
				logger.Info("startNode收到信号", "value", signalInput.NodeName)
				currentNode, exits := graph.GetNodeByName(signalInput.NodeName)
				if !exits {
					logger.Error(fmt.Sprintf("找不到下一个执行节点的定义: %s", signalInput.NodeName))
					return
				}
				if currentNode.ID == graph.EndNode.ID {
					logger.Info("已执行到结束节点")
					return
				}
				// 获取代执行节点
				logger.Info("获取待执行节点", "currentNode.Name", currentNode.Name, "signalInput.Metadata.NextBranch", signalInput.Metadata.NextBranch)
				nodes, err := graph.GetNextNodes(currentNode.Name, signalInput.Metadata.NextBranch)
				if err != nil {
					logger.Error("获取next节点失败", "error", err)
					return
				}
				logger.Info("待执行节点", "step", stepCount, "nodes", nodes, "currentNode.Name", currentNode.Name)
				// 组装待执行节点的输入数据：上一个节点的输出（以节点名称为key） + initialData（开始节点携带的数据）
				inputData := util.DeepCopyMap(initialData)
				if inputData == nil {
					logger.Error("输入参数数据有误")
					return
				}
				inputData[signalInput.NodeName] = signalInput.Data

				// 使用 workflow.Go + Future 并发
				type nodeFuture struct {
					node   nodepkg.WkFLowNode
					future workflow.Future
				}
				tasks := make([]nodeFuture, 0, len(nodes))
				// 遍历节点，开启任务
				for _, nd := range nodes {
					n := nd // 避免闭包捕获循环变量
					f, s := workflow.NewFuture(gCtx)
					tasks = append(tasks, nodeFuture{node: n, future: f})
					// 为每个分支构造独立输入副本（避免共享引用污染）
					branchInput := util.DeepCopyMap(inputData)
					if branchInput == nil {
						s.Set(nil, errors.New("inputData序列化失败"))
						return
					}
					// 开启协程，实际执行节点逻辑
					workflow.Go(gCtx, func(goCtx workflow.Context) {
						if stepCount >= maxStep {
							s.Set(nil, errors.New("over maxStep stop"))
							return
						}
						stepCount += 1
						// 在工作流绿色线程中执行节点
						res, err := executeNode(goCtx, n, branchInput, express, signalInput)
						s.Set(res, err)
					})
				}
				// 收集所有分支节点的执行结果，并在主线程统一写入状态与发送信号
				var nodeErr error
				for _, t := range tasks {
					var res *NodeExecResult
					// 节点执行失败，直接停止整体流水线
					if err := t.future.Get(gCtx, &res); err != nil {
						logger.Error("节点执行失败", "nodeName", t.node.Name, "error", err.Error())
						nodeErr = err
						break
					}

					// 针对于变量设置节点，更新workflowContext节点的数据
					if strings.Contains(t.node.Type, ".variable") && t.node.Parameters["operation"] == "set" && len(res.Data) > 0 {
						express.GetWorkflowContext().SetNodeData(nodepkg.ExpressVariablesNodeName, res.Data[0])
					}
					// 节点执行响应处理（统一在主线程）
					execNodeFinalResults[t.node.Name] = res
					// 如果当前节点是 非远程 节点，那么就触发新的通道信号（必须在工作流线程中）
					if !t.node.IsRemote {
						localChan[t.node.Name].Send(gCtx, &notify.SignalInput{
							NodeName: t.node.Name,
							Data:     res.Data,
							Metadata: res.Metadata,
						})
					}
					// 将执行结果写入其中，用于后续节点使用
					if res.Success && len(res.Data) > 0 {
						express.GetWorkflowContext().SetNodeData(t.node.Name, res.Data[0])
					}
					// todo 获取所有执行节点的响应（不论是否是本地节点还是远程节点），提取其中“新增任务数” + “已完成任务数”（这个应该都是1）
				}
				if nodeErr != nil {
					// 按需处理：可以选择直接返回，或继续处理其他分支
					logger.Error("节点执行失败", "nodeName", currentNode.Name, "error", nodeErr.Error())
					return
				}
			})
		}
		// 循环监听，直到上下文取消
		for gCtx.Err() == nil {
			selector.Select(gCtx)
		}
		logger.Info("信号监听器退出")
	})

	totalTask += 1
	// 异步发送初始数据给到信道A1
	logger.Info("开始异步发送信号")
	// 发送“开始节点”信号（通过内部通道发送），启动整体流水线
	localChan[graph.StartNode.Name].Send(childCtx, &notify.SignalInput{
		NodeName: graph.StartNode.Name,
		Data:     []map[string]interface{}{initialData},
		Metadata: notify.SignalMetadata{
			Branch:     "main",
			NextBranch: "main",
			TraceId:    workflow.GetInfo(ctx).WorkflowExecution.RunID,
			Label:      "",
		},
	})
	logger.Info("初始异步发送信号完成")
	// 等待所有Activity完成
	for {
		// 每分钟检测一次流水线是否执行完毕
		_ = workflow.Sleep(childCtx, time.Minute)
		/*
			* todo 退出逻辑
			除开始节点以外，所有节点的 发送任务数 和 接收结果数，都应该在同一个位置，而且顺序一定是先增后减。判断停止初定在循环内部，异步判定。
		*/
		if totalTask > 0 {
			logger.Info("totalTask not fninish", "任务数", totalTask)
			continue
		}
		// 退出函数
		cancelFunc()
		logger.Info("所有Activity执行完成，工作流退出")
		break
	}

	// 收集统计最终结果
	finalResult := make(map[string]interface{}) // 最终结果
	successfulNodes := 0                        // 执行成功节点次数
	failedNodes := 0                            // 执行失败节点次数
	for _, result := range execNodeFinalResults {
		if result.Success {
			successfulNodes++
		} else {
			failedNodes++
		}
	}
	// 计算跳过的节点
	finalResult["success"] = failedNodes == 0
	finalResult["executedNodes"] = len(execNodeFinalResults)
	finalResult["successfulNodes"] = successfulNodes
	finalResult["failedNodes"] = failedNodes
	finalResult["maxStep"] = maxStep
	finalResult["nodeExecutionCounts"] = execNodeCnt
	// 从图中获取工作流信息
	allNodes := graph.GetAllNodes()
	if len(allNodes) > 0 {
		finalResult["workflowId"] = allNodes[0].ID // 从第一个节点获取工作流ID信息
		finalResult["workflowName"] = graph.Workflow.Name
		finalResult["totalNodes"] = len(allNodes)
	}
	// 收集所有节点的执行结果
	nodeResults := make(map[string]interface{})
	for nodeName, result := range execNodeFinalResults {
		nodeResult := map[string]interface{}{
			"success": result.Success,
			"data":    result.Data,
		}
		if result.Error != "" {
			nodeResult["error"] = result.Error
		}
		nodeResults[nodeName] = nodeResult
	}
	finalResult["nodeResults"] = nodeResults
	// 异常情况处理和警告信息
	if failedNodes > 0 {
		logger.Warn("工作流执行完成，但有节点失败", "failedNodes", failedNodes, "totalNodes", len(allNodes))
	}
	logger.Info("通用 n8n 工作流执行完成", "totalNodes", len(execNodeFinalResults), "successfulNodes", successfulNodes, "failedNodes", failedNodes)
	return finalResult, nil
}

// ExecuteStartNode 开始节点活动
func ExecuteStartNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator, node nodepkg.WkFLowNode) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewStartNodeActivity(node, express).Execute(ctx, input)
}

// ExecuteEndNode 结束节点活动
func ExecuteEndNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator, node nodepkg.WkFLowNode) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewEndNodeActivity(node, express).Execute(ctx, input)
}

// ExecuteVariableNode 变量节点活动
func ExecuteVariableNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator, node nodepkg.WkFLowNode) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewVariableNodeActivity(node, express).Execute(ctx, input)
}

// ExecuteConditionalNode 条件节点活动
func ExecuteConditionalNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator, node nodepkg.WkFLowNode) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewConditionalNodeActivity(node, express).Execute(ctx, input)
}

// ExecutePythonDockerNode Python Docker节点活动
func ExecutePythonDockerNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator, node nodepkg.WkFLowNode) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewPythonDockerNodeActivity(node, express).Execute(ctx, input)
}
