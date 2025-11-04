package workflow

// 将n8n中的json数据，转换成temporal的执行流程

import (
	"context"
	"errors"
	"fmt"
	"github.acme.red/wego/pkg/utils/maputil/v2"
	"golang.org/x/sync/errgroup"
	"n8n2temporal/consts"
	nodepkg "n8n2temporal/node"
	"n8n2temporal/notify"
	"n8n2temporal/workflow/wkGraph"
	"strings"
	"sync/atomic"
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// NodeExecResult 节点执行结果
type NodeExecResult struct {
	NodeID   string                 `json:"node_id"`            // 节点ID
	NodeName string                 `json:"node_name"`          // 节点名称
	Success  bool                   `json:"success"`            // 执行成功与否
	Data     map[string]interface{} `json:"data"`               // 执行响应数据(不一定是节点的结果，只能说执行响应)
	Error    string                 `json:"error,omitempty"`    // 错误原因
	Metadata notify.SignalMetadata  `json:"metadata,omitempty"` // 头数据，用于透传到下一个执行节点
}

// executeNode 执行单个节点
func executeNode(ctx workflow.Context, node nodepkg.WkFLowNode, inputData map[string]interface{}, express *nodepkg.ExpressionEvaluator, signalInput *notify.SignalInput) (*NodeExecResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("开始执行节点", "nodeId", node.ID, "nodeName", node.Name, "nodeType", node.Type)
	result := &NodeExecResult{
		NodeID:   node.ID,
		Success:  false,
		Data:     make(map[string]interface{}),
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
	var execNode ExecuteNode                    // 执行节点
	var activityInput = &nodepkg.ActivityInput{ // 节点输入参数
		NodeID:     node.ID,
		NodeName:   node.Name,
		NodeType:   node.Type,
		InputData:  inputData,
		Parameters: node.Parameters,
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
		execNode = ExecuteDomainResolve

	// 自定义节点 - 支持多种自定义节点类型 / 支持ASM相关自定义节点
	case strings.HasPrefix(node.Type, "CUSTOM.") || strings.HasSuffix(node.Type, ".customNode") || strings.Contains(node.Type, "asm_"):
		execNode = ExecuteCustomNode

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
		logger.Info("节点执行成功", "nodeId", node.ID)
	}

	// 节点执行完毕后，更新元数据
	result.Metadata.Branch = result.Metadata.NextBranch
	result.Metadata.NextBranch = "main"
	// 只有condition节点的nextBranch需要修改，其他都一定是默认main
	if strings.Contains(nodeResult.NodeType, ".conditional") {
		matchBranch := maputil.GetMapValue(nodeResult.Data, "matchedConditions", nil)
		if branch, ok := matchBranch.([]string); ok && len(branch) > 0 {
			result.Metadata.NextBranch = branch[0]
		}
	}
	return result, err
}

// GenericWorkflowWithMaxStep 是带最大步数限制的通用 Temporal 工作流定义
func GenericWorkflowWithMaxStep(ctx workflow.Context, workflowJson string, initialData map[string]interface{}, maxStep int64) (map[string]interface{}, error) {
	// todo 待完成停止条件
	var totalTask atomic.Int64

	logger := workflow.GetLogger(ctx)
	logger.Info("开始执行通用 n8n 工作流", "maxStep", maxStep)

	// 创建工作流图对象
	graph, err := wkGraph.NewWkFlowGraph(workflowJson)
	if err != nil {
		return nil, fmt.Errorf("解析工作流定义失败: %v", err)
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

	// 初始化全局变量对象
	express := nodepkg.NewExpressionEvaluator(nil)

	// 创建执行上下文和数据存储
	execNodeFinalResults := make(map[string]*NodeExecResult) // 获取每个节点最后一次执行结果(同一节点可能被执行多次，这里记录该节点最后一次执行的结果)
	execNodeCnt := make(map[string]int)                      // 记录每个节点的执行次数 todo 当前还未使用，后续需在节点完成后累加
	signalChan := map[string]workflow.ReceiveChannel{}       // 信号通道映射,用于监听
	localChan := map[string]workflow.Channel{}               // 本地通道映射，用于本地发送信号

	// 创建可取消的上下文用于协程管理
	childCtx, cancelFunc := workflow.WithCancel(ctx)
	defer cancelFunc()

	// 信号通道初始化
	for _, flowNode := range graph.GetAllNodes() {
		if flowNode.IsRemote {
			signalChan[flowNode.Name] = workflow.GetSignalChannel(childCtx, flowNode.Name)
		} else {
			localCh := workflow.NewNamedBufferedChannel(childCtx, flowNode.Name, consts.DefaultChannelSize)
			signalChan[flowNode.Name] = localCh
			localChan[flowNode.Name] = localCh
		}
	}

	// 异步信号监听执行
	workflow.Go(childCtx, func(gCtx workflow.Context) {
		selector := workflow.NewSelector(gCtx)
		// 全局执行数量定义
		stepCount := atomic.Int64{}
		// TODO: 工作流中避免使用 sync/atomic 等标准并发原语；改用 workflow 原语（Future/Selector/Channel）
		//       管理并发与计数，以确保可重放的确定性。
		// 开启全局监听 todo 需要提前对map进行排序，确保访问遍历的顺序
		for nodeName, receiveCh := range signalChan {
			selector.AddReceive(receiveCh, func(c workflow.ReceiveChannel, more bool) {
				// more标识信号通道是否关闭 todo 要验证下，当more为false的时候，我需要怎么做，要不要把历史消费完
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
					logger.Error(fmt.Sprintf("找不到节点定义: %s", signalInput.NodeName))
					return
				}
				// 获取代执行节点
				nodes, err := graph.GetNextNodes(currentNode.Name, signalInput.Metadata.NextBranch)
				if err != nil {
					logger.Error("获取next节点失败", "error", err)
					return
				}
				logger.Info("待执行节点", "step", stepCount.Load(), "nodes", nodes)
				// 组装待执行节点的输入数据：上一个节点的输出（以节点名称为key） + initialData（开始节点携带的数据）
				inputData := initialData
				// TODO: 并发污染风险：复用并修改同一个 initialData（map）在并发场景会互相覆盖，影响重放的确定性；
				//       后续应为每个分支构造独立副本，再填充当前节点相关数据。
				inputData[signalInput.NodeName] = signalInput.Data
				// 节点执行。开启并发，对于该节点的多个下级节点，应该是并发执行的 todo 这里的标准库并发可能会导致预期之外的结果。
				errGroup, _ := errgroup.WithContext(context.Background())
				// TODO: 非确定性风险：工作流不应使用标准库并发（errgroup/goroutine/context.Background）；
				//       后续需改为使用 workflow.Go + workflow.Future/Selector 实现并发与等待，避免破坏重放。
				errGroup.SetLimit(len(nodes))
				for _, node := range nodes {
					// TODO: 注意：在标准 goroutine 中访问工作流状态/Channel 会导致非确定性；应在 workflow.Go 绿色线程中执行。
					errGroup.Go(func() error {
						// 判断是否超过节点执行数量上限
						if stepCount.Load() >= maxStep {
							return errors.New("over maxStep stop")
						}
						// 实际执行节点
						stepCount.Add(1)
						result, err := executeNode(ctx, node, inputData, express, signalInput)
						if err != nil {
							return err
						}
						// 针对于变量设置节点，更新workflowContext节点的数据
						if strings.Contains(node.Type, ".variable") && node.Parameters["operation"] == "set" {
							// TODO: 并发写入风险：WorkflowContext 内部使用普通 map；在真实 goroutine 并发写入会产生竞态并破坏确定性。
							express.GetWorkflowContext().SetNodeData(nodepkg.ExpressVariablesNodeName, result.Data)
						}
						// 节点执行响应处理
						// TODO: 数据竞争风险：在标准 goroutine 并发下对 execNodeFinalResults（普通 map）写入存在竞态；
						execNodeFinalResults[node.Name] = result
						// 如果当前节点是 非远程 节点，那么就触发新的通道信号
						if !node.IsRemote {
							// TODO: 非法用法：从标准 goroutine 操作 workflow.Channel；Channel 发送必须在 workflow.Go 线程中进行。
							localChan[node.Name].Send(ctx, &notify.SignalInput{
								NodeName: node.Name,
								Data:     result.Data,
								Metadata: result.Metadata,
							})
						}
						return nil
					})
				}
				// TODO: 设计建议：在 selector 回调内阻塞等待所有分支完成会降低事件循环响应；
				//       后续可将重处理下沉到独立 workflow.Go 中，回调只负责轻量入队。
				if err := errGroup.Wait(); err != nil {
					logger.Error("节点执行失败", "nodeName", currentNode.Name, "error", err.Error())
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

	// 设置当前待完成任务数
	// TODO: 退出条件未实现：totalTask 没有递减逻辑，若后续依赖该计数做退出控制将无法生效；
	//       建议使用 futures 汇总或在完成时递减以形成有效的退出条件。
	totalTask.Add(1)
	// 异步发送初始数据给到信道A1
	logger.Info("开始异步发送信号完成")
	// todo 发送 开始节点 信号（通过通道发送）
	logger.Info("初始异步发送信号完成")
	// 等待所有Activity完成
	for {
		workflow.Sleep(ctx, time.Second*5)
		// 检查是否所有Activity都已执行
		if totalTask.Load() > 0 {
			logger.Info("totalTask not fninish", "任务数", totalTask.Load())
			continue
		}
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

	// 取消所有子协程
	cancelFunc()

	logger.Info("通用 n8n 工作流执行完成", "totalNodes", len(execNodeFinalResults), "successfulNodes", successfulNodes, "failedNodes", failedNodes)
	return finalResult, nil
}

// ExecuteNode 执行节点格式定义
type ExecuteNode func(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator, node nodepkg.WkFLowNode) (*nodepkg.ActivityOutput, error)

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

// ExecuteDomainResolve 域名解析节点活动
func ExecuteDomainResolve(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator, node nodepkg.WkFLowNode) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewDomainResolveActivity(node, express).Execute(ctx, input)
}

// ExecuteCustomNode 自定义节点活动
func ExecuteCustomNode(ctx context.Context, input *nodepkg.ActivityInput, express *nodepkg.ExpressionEvaluator, node nodepkg.WkFLowNode) (*nodepkg.ActivityOutput, error) {
	return nodepkg.NewCustomNodeActivity(node, express).Execute(ctx, input)
}
