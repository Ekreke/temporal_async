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

	"github.com/bytedance/sonic"

	"github.acme.red/wego/pkg/utils/arrayutil/v2"

	"github.acme.red/wego/pkg/utils/maputil/v2"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// ExecNodeInput 执行节点输入
// 封装单个节点执行所需的上下文与数据：
// - Node：待执行的工作流节点定义
// - InputData：节点实际输入（来自初始数据与前序节点输出）
// - Express：全局表达式求值器（含工作流上下文）
// - SignalInput：触发当前执行的信号（上个节点名称与其输出、分支元数据）
type ExecNodeInput struct {
	Node        nodepkg.WkFLowNode           // 待执行节点
	InputData   []map[string]interface{}     // 节点输入参数
	Express     *nodepkg.ExpressionEvaluator // 全局节点表达式
	SignalInput *notify.SignalData           // 信号输入(包含上一个节点和其执行结果)
}

// ActivityExecResult 节点执行结果
// 工作流侧对 Activity 执行结果的统一封装与透传：
// - Success/Data/Error：节点执行是否成功与输出数据
// - Metadata：分支元数据（影响下一跳分支选择）
// - AddTaskNum/FinishTaskNum：用于 totalTask 的增减统计（支持流式、多次返回）
type ActivityExecResult struct {
	NodeID        string                   `json:"node_id"`            // 节点ID
	NodeName      string                   `json:"node_name"`          // 节点名称
	Success       bool                     `json:"success"`            // 执行成功与否
	Data          []map[string]interface{} `json:"data"`               // 执行响应数据(不一定是节点的结果，只能说执行响应)
	Error         string                   `json:"error,omitempty"`    // 错误原因
	Metadata      notify.SignalMetadata    `json:"metadata,omitempty"` // 头数据，用于透传到下一个执行节点
	AddTaskNum    int                      `json:"add_task_num"`       // 新增任务数
	FinishTaskNum int                      `json:"finish_task_num"`    // 完成任务数
}

// queuedTask 任务排队项。封装尚未被调度执行的节点及其输入
type queuedTask struct {
	node      nodepkg.WkFLowNode
	execInput *ExecNodeInput
}

// orchestrator 编排器
// 负责：
// - 通道初始化与信号监听（selector）
// - 节点调度与结果回调处理（AddFuture）
// - 并发窗口控制（selectorMaxFutures 与阻塞等待）
// - 统计指标与退出条件管理
type orchestrator struct {
	ctx                   workflow.Context                   // 工作流上下文（可取消子上下文）
	graph                 *wkGraph.WkFlowGraph               // 工作流图对象（节点与连接关系）
	express               *nodepkg.ExpressionEvaluator       // 全局表达式求值器（含工作流上下文）
	maxStep               int64                              // 最大步数限制（环路安全阈值，为0表示不限制）
	selector              workflow.Selector                  // 事件选择器（接收信号与Future回调）
	signalChan            map[string]workflow.ReceiveChannel // 节点信号通道映射（远程节点使用）
	localChan             map[string]workflow.Channel        // 本地缓冲通道映射（本地节点使用）
	execNodeFinalResults  map[string]*ActivityExecResult     // 每节点最后一次执行结果
	activityExecUnqIds    map[string][]string                // 执行ID对应已处理的数据ID（去重用）
	totalTask             int64                              // 在途活动计数（调度前加1，完成回调后减1）
	stepCount             int64                              // 已调度的活动总数（用于统计与限步）
	selectorMaxFutures    int                                // 选择器窗口上限（已注册未完成Future的最大值）
	registeredFutures     int                                // 当前已注册但未完成的Future数量
	pending               []queuedTask                       // 待调度队列（阻塞窗口模式下通常为空）
	peakRegisteredFutures int                                // 峰值窗口占用（最大registeredFutures）
	peakPending           int                                // 峰值队列长度
	selectTicks           int64                              // 选择器轮询次数（用于历史长度与续跑触发）
	inflightSignals       int64                              // 处理中的信号计数（AddReceive开始++、调度完成后--）
	exitStableChecks      int                                // 退出去抖所需连续稳定次数
	exitStableCounter     int                                // 当前连续满足三条件的计数
	continueRequested     bool                               // 触发了继续运行请求（continue-as-new）
	continueThreshold     int                                // 触发continue-as-new的选择器轮询阈值
}

// newOrchestrator 初始化编排器。创建 selector 与各类映射；设置窗口上限与统计初值
func newOrchestrator(ctx workflow.Context, graph *wkGraph.WkFlowGraph, express *nodepkg.ExpressionEvaluator, maxStep int64) *orchestrator {
	return &orchestrator{
		ctx:                  ctx,
		graph:                graph,
		express:              express,
		maxStep:              maxStep,
		selector:             workflow.NewSelector(ctx),
		signalChan:           map[string]workflow.ReceiveChannel{},
		localChan:            map[string]workflow.Channel{},
		execNodeFinalResults: make(map[string]*ActivityExecResult),
		activityExecUnqIds:   make(map[string][]string),
		selectorMaxFutures:   consts.SelectorMaxFutures,
		pending:              make([]queuedTask, 0),
		exitStableChecks:     2,
		continueThreshold:    consts.ContinueAsNewSelectTicks,
	}
}

// initChannels 初始化所有节点的监听通道（远程节点用信号通道，本地节点用缓冲通道）并返回排序后的通道名称数组
func (o *orchestrator) initChannels() []string {
	// 特殊处理，根节点（root）需要监听远程信号节点
	o.signalChan["root"] = workflow.GetSignalChannel(o.ctx, "root")
	for _, flowNode := range o.graph.GetAllNodes() {
		if flowNode.IsRemote {
			o.signalChan[flowNode.Name] = workflow.GetSignalChannel(o.ctx, flowNode.Name)
		} else {
			localCh := workflow.NewNamedBufferedChannel(o.ctx, flowNode.Name, consts.DefaultChannelSize)
			o.signalChan[flowNode.Name] = localCh
			o.localChan[flowNode.Name] = localCh
		}
	}
	var arr = make([]string, 0, len(o.signalChan))
	for name := range o.signalChan {
		arr = append(arr, name)
	}
	slices.Sort(arr)
	return arr
}

// startSignalLoop 为每个通道注册接收回调，驱动主事件循环；包含：信号验参、重复数据去重、下一跳节点解析与逐分支调度
func (o *orchestrator) startSignalLoop(signalArr []string) {
	logger := workflow.GetLogger(o.ctx)
	for _, signalName := range signalArr {
		nodeName := signalName
		logger.Warn("开始循环信号：" + nodeName)
		o.selector.AddReceive(o.signalChan[nodeName], func(c workflow.ReceiveChannel, more bool) {
			if !more {
				logger.Info("信号通道已关闭", "nodeName", nodeName)
				return
			}
			o.inflightSignals++
			defer func() { o.inflightSignals-- }()
			// 输入信号
			var signalInput *notify.SignalData
			c.Receive(o.ctx, &signalInput)
			if err := signalInput.Validate(); err != nil {
				logger.Error("信号数据解析失败:" + err.Error())
				return
			}
			// 重复信号数据过滤
			if len(signalInput.DataId) > 0 {
				atyExecIds := o.activityExecUnqIds[signalInput.ActivityExecId]
				repeatIds := util.InArraysRepeat(atyExecIds, signalInput.DataId)
				if len(repeatIds) == len(signalInput.DataId) {
					logger.Warn("接收到重复信号")
					return
				}
				if len(repeatIds) > 0 {
					var newData = make([]map[string]interface{}, 0)
					for k, data := range signalInput.Data {
						if arrayutil.InArray(k, repeatIds) {
							continue
						}
						newData = append(newData, data)
					}
					signalInput.Data = newData
				}
				atyExecIds = append(atyExecIds, signalInput.DataId...)
				o.activityExecUnqIds[signalInput.ActivityExecId] = arrayutil.Duplicate(atyExecIds)
			}
			// 获取当前节点
			logger.Info("startNode收到信号", "value", signalInput.NodeName)
			currentNode, exits := o.graph.GetNodeByName(signalInput.NodeName)
			if !exits {
				logger.Error(fmt.Sprintf("找不到下一个执行节点的定义: %s", signalInput.NodeName))
				return
			}
			if currentNode.ID == o.graph.EndNode.ID {
				logger.Info("已执行到结束节点")
				return
			}
			// 获取待执行节点
			logger.Info("获取待执行节点", "currentNode.Name", currentNode.Name, "signalInput.Metadata.NextBranch", signalInput.Metadata.NextBranch)
			nodes, err := o.graph.GetNextNodes(currentNode.Name, signalInput.Metadata.NextBranch)
			if err != nil {
				logger.Error("获取next节点失败", "error", err)
				return
			}
			logger.Info("待执行节点", "step", o.stepCount, "nodes", nodes, "currentNode.Name", currentNode.Name)
			signalInput.Data = nil

			// 顺序执行每一个节点
			for _, nd := range nodes {
				// 解析节点的inputData
				branchInput := o.analysisParamInput(nd)
				if len(branchInput) < 1 {
					logger.Warn("没有拿到上一个节点的执行结果")
				}
				o.enqueueOrSchedule(nd, branchInput, signalInput)
			}
		})
	}
}

// 解析任务参数
func (o *orchestrator) analysisParamInput(nd nodepkg.WkFLowNode) []map[string]interface{} {
	var branchInput = make([]map[string]interface{}, 0)
	if nd.IsRemote {
		if strings.HasPrefix(nd.ParametersSource, "$var.") {
			d, ok := o.express.GetWorkflowContext().GetNodeDataKV(nodepkg.ExpressVariablesNodeName, strings.TrimPrefix(nd.ParametersSource, "$var."))
			if ok {
				v, o := d.([]map[string]interface{})
				if o {
					branchInput = util.DeepCopy[string, interface{}](v)
				}
			}
		} else if strings.HasPrefix(nd.ParametersSource, "$global.") {
			d, ok := o.express.GetWorkflowContext().GetNodeDataKV(nodepkg.ExpressGlobalNodeName, strings.TrimPrefix(nd.ParametersSource, "$global."))
			if ok {
				v, ok := d.([]interface{})
				if ok {
					var mData = make([]map[string]interface{}, 0, len(v))
					marDt, _ := sonic.Marshal(v)
					_ = sonic.Unmarshal(marDt, &mData)
					branchInput = util.DeepCopy[string, interface{}](mData)
				}
			}
		} else {
			actRes := o.execNodeFinalResults[nd.ParametersSource]
			if actRes != nil {
				branchInput = util.DeepCopy[string, interface{}](actRes.Data)
			}
		}
	}
	return branchInput
}

// 任务调度（阻塞式窗口控制）当窗口已满时，直接阻塞等待空闲出现，避免内存膨胀；否则立刻调度执行
func (o *orchestrator) enqueueOrSchedule(n nodepkg.WkFLowNode, branchInput []map[string]interface{}, signalInput *notify.SignalData) {
	// 执行超过最大步数则需要停止（设置了才有效，非环可以取消这个值）
	if o.maxStep > 0 && o.stepCount >= o.maxStep {
		return
	}
	// 若窗口已满，阻塞等待空闲（不入队，避免队列积压导致内存扩大）
	if o.registeredFutures >= o.selectorMaxFutures {
		_ = workflow.Await(o.ctx, func() bool { return o.registeredFutures < o.selectorMaxFutures })
	}
	o.totalTask += 1
	o.stepCount += 1
	o.scheduleNode(n, branchInput, signalInput)
}

// scheduleNode 选择合适的 Activity 并发起执行；为其 Future 注册非阻塞结果回调
func (o *orchestrator) scheduleNode(n nodepkg.WkFLowNode, branchInput []map[string]interface{}, signalInput *notify.SignalData) {
	logger := workflow.GetLogger(o.ctx)
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour * 1,
		HeartbeatTimeout:    time.Second * 30,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	actCtx := workflow.WithActivityOptions(o.ctx, activityOptions)
	var execID string
	rand := workflow.SideEffect(actCtx, func(ctx workflow.Context) interface{} {
		return fmt.Sprintf("acty_%s_%s", util.UUID(), time.Now().Format("20060102150405"))
	})
	_ = rand.Get(&execID)
	wkInfo := workflow.GetInfo(actCtx)
	activityInput := &nodepkg.ActivityInput{
		ExecID:      execID,
		NodeID:      n.ID,
		NodeName:    n.Name,
		NodeType:    n.Type,
		InputData:   branchInput,
		Parameters:  n.Parameters,
		SignalInput: signalInput,
		WorkflowID:  wkInfo.WorkflowExecution.ID,
		ExecutionID: wkInfo.WorkflowExecution.RunID,
		StreamRsp:   n.IsRemote,
	}
	var execNode interface{}
	switch {
	case strings.HasSuffix(n.Type, ".start") || strings.HasSuffix(n.Type, ".manualTrigger"):
		execNode = ExecuteStartNode
	case strings.HasSuffix(n.Type, ".variable"):
		execNode = ExecuteVariableNode
	case strings.HasSuffix(n.Type, ".pythonDocker") || strings.HasSuffix(n.Type, ".code"):
		execNode = ExecutePythonDockerNode
	case strings.HasSuffix(n.Type, ".condition") || strings.HasSuffix(n.Type, ".if") || strings.HasSuffix(n.Type, ".conditional"):
		execNode = ExecuteConditionalNode
	case strings.HasSuffix(n.Type, ".domainResolve") || strings.HasSuffix(n.Type, ".asmDomainResolve") || strings.HasSuffix(n.Type, ".asm_domain_resolve"):
		execNode = "DomainResolveActivity"
	case strings.HasSuffix(n.Type, ".end"):
		execNode = ExecuteEndNode
	default:
		logger.Error("不支持的节点类型", "nodeType", n.Type, "nodeName", n.Name, "nodeId", n.ID)
		return
	}
	fut := workflow.ExecuteActivity(actCtx, execNode, activityInput, o.express, n)
	o.registeredFutures += 1
	nn := n
	exec := execID
	o.selector.AddFuture(fut, func(f workflow.Future) {
		var res *ActivityExecResult
		err := f.Get(o.ctx, &res)
		o.onNodeDone(nn, res, err, exec)
		o.registeredFutures -= 1
		o.totalTask -= 1
		o.tryStartPending()
	})
	if o.registeredFutures > o.peakRegisteredFutures {
		o.peakRegisteredFutures = o.registeredFutures
	}
}

// tryStartPending 兼容队列模式（当前为阻塞模式，下列逻辑为空窗补注册）
func (o *orchestrator) tryStartPending() {
	for o.registeredFutures < o.selectorMaxFutures && len(o.pending) > 0 {
		q := o.pending[0]
		o.pending = o.pending[1:]
		o.stepCount += 1
		o.scheduleNode(q.node, q.execInput.InputData, q.execInput.SignalInput)
	}
}

// onNodeInput 统一处理节点输入? // todo 这里要思考下，能否统一所有节点的输入，并且保证节点的正常运行
func (o *orchestrator) onNodeInput(n nodepkg.WkFLowNode, res *ActivityExecResult, err error, execID string) {

}

// onNodeDone 统一处理节点结果
func (o *orchestrator) onNodeDone(n nodepkg.WkFLowNode, res *ActivityExecResult, err error, execID string) {
	logger := workflow.GetLogger(o.ctx)
	if err != nil {
		logger.Error("节点执行失败", "nodeName", n.Name, "error", err.Error())
		return
	}
	if res == nil {
		return
	}
	res.Metadata.Branch = res.Metadata.NextBranch
	res.Metadata.NextBranch = "main"
	// - 分支元数据更新（conditional）
	if strings.Contains(n.Type, ".conditional") && len(res.Data) > 0 {
		matchBranch := maputil.GetMapValue(res.Data[0], "outputPaths", nil)
		if branch, ok := matchBranch.(string); ok {
			res.Metadata.NextBranch = branch
		}
	}
	// - 变量上下文写入（variable.set）
	if strings.Contains(n.Type, ".variable") && n.Parameters["operation"] == "set" && len(res.Data) > 0 {
		o.express.GetWorkflowContext().SetNodeData(nodepkg.ExpressVariablesNodeName, res.Data[0])
	}
	// 开始节点写入
	if strings.Contains(n.Type, ".start") && len(n.Parameters) > 0 {
		o.express.GetWorkflowContext().SetNodeData(nodepkg.ExpressGlobalNodeName, n.Parameters)
	}
	// 存储结果
	o.execNodeFinalResults[n.Name] = res
	// 非远程节点本地信号透传
	if !n.IsRemote {
		o.localChan[n.Name].Send(o.ctx, &notify.SignalData{
			ActivityExecId: execID,
			NodeName:       n.Name,
			Data:           res.Data,
			Metadata:       res.Metadata,
		})
	}
	if res.Success && len(res.Data) > 0 {
		o.express.GetWorkflowContext().SetNodeData(n.Name, res.Data[0])
	}
}

// ContinuationState 续传状态
type ContinuationState struct {
	WorkflowContext      map[string]interface{}         // 工作流上下文
	ExecNodeFinalResults map[string]*ActivityExecResult // 已执行节点的最终结果（节点ID+执行结果）
	ActivityExecUnqIds   map[string][]string            // 已执行节点的去重集合（节点ID+执行ID）
	StepCount            int64                          // 当前已执行步数
}

// GenericWorkflowInput 通用工作流输入参数
type GenericWorkflowInput struct {
	WorkflowJSON             string                 // 工作流 JSON 定义
	InitialData              map[string]interface{} // 初始化数据
	MaxStep                  int64                  // 最大步数，无环可设置为0，有环则必须设置
	Continuation             *ContinuationState     // 续跑快照（上下文、执行结果、去重集合、步数）
	ContinueAsNewSelectTicks int                    // 续传阈值，超过该值触发 continue-as-new
}

// GenericWorkflowWithMaxStep 带最大步数限制的通用 Temporal 工作流定义
// 要点：
// - 采用单 selector 统一接收信号与 Future 回调，保证非阻塞接收
// - 当窗口满时阻塞等待空闲，避免队列积压导致内存扩大
// - 退出条件：totalTask==0 且已无注册 Future（支持流式输出场景）
// - continue-as-new：基于 selectTicks 阈值触发以防历史事件过多
func GenericWorkflowWithMaxStep(ctx workflow.Context, in GenericWorkflowInput) (map[string]interface{}, error) {
	express := nodepkg.NewExpressionEvaluator(nil)
	if in.Continuation != nil && in.Continuation.WorkflowContext != nil {
		wc := nodepkg.NewWorkflowContext()
		wc.Context = in.Continuation.WorkflowContext
		express.SetWorkflowContext(wc)
	}
	// 工作流图解析
	if in.InitialData == nil {
		return nil, errors.New("初始化参数不能为空")
	}
	logger := workflow.GetLogger(ctx)
	logger.Info("开始执行通用 n8n 工作流", "maxStep", in.MaxStep)
	// 创建工作流图对象
	graph, err := wkGraph.NewWkFlowGraph(in.WorkflowJSON)
	if err != nil {
		return nil, fmt.Errorf("解析工作流定义失败: %v", err)
	}
	if graph.StartNode.Check() != nil || graph.EndNode.Check() != nil {
		return nil, errors.New("缺少开始或结束节点")
	}
	// 检测工作流环
	rings := graph.DetectRings()
	if rings.HasRings && in.MaxStep < 1 {
		return nil, fmt.Errorf("工作流中存在环，但是没有设置最大步数")
	}
	if !rings.ValidateRings() {
		return nil, fmt.Errorf("工作流中存在没有出口的环")
	}
	logger.Info("工作流图解析完成", "nodeCount", len(graph.GetAllNodes()))

	// 创建可取消的上下文用于协程管理
	childCtx, cancelFunc := workflow.WithCancel(ctx)
	defer cancelFunc()
	o := newOrchestrator(childCtx, graph, express, in.MaxStep)
	if in.Continuation != nil {
		if in.Continuation.ExecNodeFinalResults != nil {
			o.execNodeFinalResults = in.Continuation.ExecNodeFinalResults
		}
		if in.Continuation.ActivityExecUnqIds != nil {
			o.activityExecUnqIds = in.Continuation.ActivityExecUnqIds
		}
		if in.Continuation.StepCount > 0 {
			o.stepCount = in.Continuation.StepCount
		}
	}
	signalArr := o.initChannels()
	logger.Info("信号数组", "signalArr", signalArr)
	workflow.Go(childCtx, func(gCtx workflow.Context) {
		o.selector = workflow.NewSelector(gCtx)
		o.startSignalLoop(signalArr)
		for gCtx.Err() == nil {
			o.selector.Select(gCtx)
			o.selectTicks++
			if int(o.selectTicks) > o.continueThreshold {
				o.continueRequested = true
			}
		}
		logger.Info("信号监听器退出")
	})

	// 发送开始信号，用于触发start节点，改为启动的时候，由外部发送信号
	logger.Info("开始异步发送信号")
	if in.Continuation == nil {
		o.localChan[graph.StartNode.Name].Send(childCtx, &notify.SignalData{
			ActivityExecId: "aty_1",
			NodeName:       "root",
			Data:           []map[string]interface{}{in.InitialData},
			Metadata: notify.SignalMetadata{
				Branch:     "main",
				NextBranch: "main",
				TraceId:    workflow.GetInfo(ctx).WorkflowExecution.ID,
				Label:      "",
			},
		})
	}
	logger.Info("初始异步发送信号完成")

	if in.ContinueAsNewSelectTicks > 0 {
		o.continueThreshold = in.ContinueAsNewSelectTicks
	}
	err = workflow.Await(childCtx, func() bool {
		return ((o.totalTask == 0 && o.registeredFutures == 0 && o.inflightSignals == 0) && o.selectTicks > 0) || o.continueRequested
	})
	if err != nil {
		return nil, err
	}
	if o.continueRequested {
		logger.Info("触发 continue-as-new 以控制历史事件规模", "selectTicks", o.selectTicks)
		state := &ContinuationState{
			WorkflowContext:      express.GetWorkflowContext().GetAllContext(),
			ExecNodeFinalResults: o.execNodeFinalResults,
			ActivityExecUnqIds:   o.activityExecUnqIds,
			StepCount:            o.stepCount,
		}
		nextIn := GenericWorkflowInput{
			WorkflowJSON:             in.WorkflowJSON,
			InitialData:              in.InitialData,
			MaxStep:                  in.MaxStep,
			Continuation:             state,
			ContinueAsNewSelectTicks: o.continueThreshold,
		}
		return nil, workflow.NewContinueAsNewError(childCtx, GenericWorkflowWithMaxStep, nextIn)
	}
	logger.Info("所有Activity执行完成，工作流退出")

	// 收集统计最终结果
	var (
		finalResult     = make(map[string]interface{}) // 最终结果
		successfulNodes = 0                            // 执行成功节点次数
		failedNodes     = 0                            // 执行失败节点次数
	)
	{
		for _, result := range o.execNodeFinalResults {
			if result.Success {
				successfulNodes++
			} else {
				failedNodes++
			}
		}
		finalResult["success"] = failedNodes == 0
		finalResult["executedNodes"] = len(o.execNodeFinalResults)
		finalResult["successfulNodes"] = successfulNodes
		finalResult["failedNodes"] = failedNodes
		finalResult["maxStep"] = in.MaxStep
		finalResult["selectorMaxFutures"] = o.selectorMaxFutures
		finalResult["registeredPeak"] = o.peakRegisteredFutures
		finalResult["pendingPeak"] = o.peakPending
		finalResult["selectTicks"] = o.selectTicks
		finalResult["windowSaturated"] = o.peakRegisteredFutures >= o.selectorMaxFutures
		finalResult["inflightSignals"] = o.inflightSignals
		finalResult["exitStableChecks"] = o.exitStableChecks
		// 从图中获取工作流信息
		allNodes := graph.GetAllNodes()
		if len(allNodes) > 0 {
			finalResult["workflowId"] = workflow.GetInfo(ctx).WorkflowExecution.ID // 从第一个节点获取工作流ID信息
			finalResult["workflowName"] = graph.Workflow.Name
			finalResult["totalNodes"] = len(allNodes)
		}
		// 收集所有节点的执行结果
		nodeResults := make(map[string]interface{})
		for nodeName, result := range o.execNodeFinalResults {
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
	}
	logger.Info("通用 n8n 工作流执行完成", "totalNodes", len(o.execNodeFinalResults), "successfulNodes", successfulNodes, "failedNodes", failedNodes)
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

// executeNode 执行单个节点（阻塞获取结果，暂不使用）
func executeNode(ctx workflow.Context, input *ExecNodeInput) (*ActivityExecResult, error) {
	logger := workflow.GetLogger(ctx)
	logger.Info("开始执行节点", "nodeId", input.Node.ID, "nodeName", input.Node.Name, "nodeType", input.Node.Type)
	result := &ActivityExecResult{
		NodeID:   input.Node.ID,
		Success:  false,
		Data:     make([]map[string]interface{}, 0),
		Metadata: input.SignalInput.Metadata,
	}

	// 设置活动选项
	activityOptions := workflow.ActivityOptions{
		StartToCloseTimeout: time.Hour * 1,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	ctx = workflow.WithActivityOptions(ctx, activityOptions)
	// 获取节点执行ID标识
	var activityExecId string
	execRandom := workflow.SideEffect(ctx, func(ctx workflow.Context) interface{} {
		return fmt.Sprintf("acty_%s_%s", util.UUID(), time.Now().Format("20060102150405"))
	})
	err := execRandom.Get(&activityExecId)
	if err != nil {
		return nil, err
	}
	// 根据节点类型选择相应的活动
	var nodeResult = &nodepkg.ActivityOutput{}  // 节点执行结果
	var execNode interface{}                    // 执行节点
	var wkInfo = workflow.GetInfo(ctx)          // 获取工作流info信息
	var activityInput = &nodepkg.ActivityInput{ // 节点输入参数
		ExecID:   activityExecId,
		NodeID:   input.Node.ID,
		NodeName: input.Node.Name,
		NodeType: input.Node.Type,
		//InputData:   input.InputData,
		Parameters:  input.Node.Parameters,
		SignalInput: input.SignalInput,
		WorkflowID:  wkInfo.WorkflowExecution.ID,
		ExecutionID: wkInfo.WorkflowExecution.RunID,
	}
	switch {
	// 开始节点
	case strings.HasSuffix(input.Node.Type, ".start") || strings.HasSuffix(input.Node.Type, ".manualTrigger"):
		execNode = ExecuteStartNode

	// 变量节点
	case strings.HasSuffix(input.Node.Type, ".variable"):
		execNode = ExecuteVariableNode

	// Python代码执行节点
	case strings.HasSuffix(input.Node.Type, ".pythonDocker") || strings.HasSuffix(input.Node.Type, ".code"):
		// Python 代码执行节点 - 使用Python Docker节点
		execNode = ExecutePythonDockerNode

	// 条件判断节点
	case strings.HasSuffix(input.Node.Type, ".condition") || strings.HasSuffix(input.Node.Type, ".if") || strings.HasSuffix(input.Node.Type, ".conditional"):
		// IF 条件节点 - 使用统一条件节点
		execNode = ExecuteConditionalNode

	// 域名解析节点
	case strings.HasSuffix(input.Node.Type, ".domainResolve") || strings.HasSuffix(input.Node.Type, ".asmDomainResolve") || strings.HasSuffix(input.Node.Type, ".asm_domain_resolve"):
		execNode = "DomainResolveActivity"

	// 结束节点
	case strings.HasSuffix(input.Node.Type, ".end"):
		execNode = ExecuteEndNode

	default:
		logger.Error("不支持的节点类型", "nodeType", input.Node.Type, "nodeName", input.Node.Name, "nodeId", input.Node.ID)
	}
	// 执行实际节点
	if execNode != nil {
		// TODO: 风险提示：将 express（含 WorkflowContext）作为活动入参可能导致 payload 过大，
		//       且为“上下文快照”语义，易与“最新状态”预期不一致。考虑在工作流侧先完成表达式求值，仅传递必要数据。
		err = workflow.ExecuteActivity(ctx, execNode, activityInput, input.Express, input.Node).Get(ctx, &nodeResult)
	} else {
		err = fmt.Errorf("未定义节点类型: %s (节点名称: %s)", input.Node.Type, input.Node.Name)
	}
	// 统一处理响应
	if err != nil {
		result.Error = err.Error()
		logger.Error("节点执行失败", "nodeId", input.Node.ID, "error", err)
	} else {
		result.Success = nodeResult.Success
		result.Data = nodeResult.Data
		//result.AddTaskNum = nodeResult.AddTaskNum
		//result.FinishTaskNum = nodeResult.FinishTaskNum
		logger.Info("节点执行成功", "nodeId", input.Node.ID)
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
