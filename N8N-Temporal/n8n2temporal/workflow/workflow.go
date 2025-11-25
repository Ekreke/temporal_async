package workflow

// 将n8n中的json数据，转换成temporal的执行流程

import (
	"errors"
	"fmt"
	"github.com/spf13/cast"
	"go.temporal.io/api/enums/v1"
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
	tempWorkflow "go.temporal.io/sdk/workflow"
)

// ExecNodeInput 执行节点输入，封装单个节点执行所需的上下文与数据
type ExecNodeInput struct {
	Node        nodepkg.WkFLowNode           // 待执行节点
	InputData   []map[string]interface{}     // 节点输入参数
	Express     *nodepkg.ExpressionEvaluator // 全局节点表达式
	SignalInput *notify.SignalData           // 信号输入(包含上一个节点和其执行结果)
}

// ActivityExecResult 节点执行结果
type ActivityExecResult struct {
	NodeID   string                   `json:"node_id"`            // 节点ID
	NodeName string                   `json:"node_name"`          // 节点名称
	Success  bool                     `json:"success"`            // 执行成功与否
	Data     []map[string]interface{} `json:"data"`               // 执行响应数据(不一定是节点的结果，只能说执行响应)
	Error    string                   `json:"error,omitempty"`    // 错误原因
	Metadata notify.SignalMetadata    `json:"metadata,omitempty"` // 元数据，用于透传到下一个执行节点
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
	ctx                   tempWorkflow.Context                   // 工作流上下文（可取消子上下文）
	graph                 *wkGraph.WkFlowGraph                   // 工作流图对象（节点与连接关系）
	express               *nodepkg.ExpressionEvaluator           // 全局表达式求值器（含工作流上下文）
	maxStep               int64                                  // 最大步数限制（环路安全阈值，为0表示不限制）
	selector              tempWorkflow.Selector                  // 事件选择器（接收信号与Future回调）
	signalChan            map[string]tempWorkflow.ReceiveChannel // 节点信号通道映射（远程节点使用）
	localChan             map[string]tempWorkflow.Channel        // 本地缓冲通道映射（本地节点使用）
	execNodeFinalResults  map[string]*ActivityExecResult         // 每节点最后一次执行结果
	activityExecUnqIds    map[string][]string                    // 执行ID对应已处理的数据ID（去重用）
	totalTask             int64                                  // 在途活动计数（调度前加1，完成回调后减1）
	stepCount             int64                                  // 已调度的活动总数（用于统计与限步）
	selectorMaxFutures    int                                    // 选择器窗口上限（已注册未完成Future的最大值）
	registeredFutures     int                                    // 当前已注册但未完成的Future数量
	pending               []queuedTask                           // 待调度队列（阻塞窗口模式下通常为空）
	peakRegisteredFutures int                                    // 峰值窗口占用（最大registeredFutures）
	peakPending           int                                    // 峰值队列长度
	selectTicks           int64                                  // 选择器轮询次数（用于历史长度与续跑触发）
	inflightSignals       int64                                  // 处理中的信号计数（AddReceive开始++、调度完成后--）
	exitStableChecks      int                                    // 退出去抖所需连续稳定次数
	exitStableCounter     int                                    // 当前连续满足三条件的计数
	continueRequested     bool                                   // 触发了继续运行请求（continue-as-new）
	continueThreshold     int                                    // 触发continue-as-new的选择器轮询阈值
	outMode               string                                 // 工作流响应收集模式。目前仅支持内存：memory（内存临时保存，最后统一返回）
	outNodes              []string                               // 需要结果节点(节点名称)
	outData               map[string][]map[string]interface{}    // 输出节点结果存储
}

// newOrchestrator 初始化编排器。创建 selector 与各类映射；设置窗口上限与统计初值
func newOrchestrator(ctx tempWorkflow.Context, graph *wkGraph.WkFlowGraph, express *nodepkg.ExpressionEvaluator, in GenericWorkflowInput) (*orchestrator, error) {
	outNodes, err := cast.ToStringSliceE(in.InitialData["out_nodes"])
	if err != nil {
		return nil, errors.New("out_nodes error: " + err.Error())
	}
	return &orchestrator{
		ctx:                  ctx,
		graph:                graph,
		express:              express,
		maxStep:              in.MaxStep,
		selector:             tempWorkflow.NewSelector(ctx),
		signalChan:           map[string]tempWorkflow.ReceiveChannel{},
		localChan:            map[string]tempWorkflow.Channel{},
		execNodeFinalResults: make(map[string]*ActivityExecResult),
		activityExecUnqIds:   make(map[string][]string),
		selectorMaxFutures:   consts.SelectorMaxFutures,
		pending:              make([]queuedTask, 0),
		exitStableChecks:     2,
		continueThreshold:    consts.ContinueAsNewSelectTicks,
		outMode:              "memory", // todo 这个值暂时不做判断，先写死，后面丰富了再透传
		outNodes:             outNodes,
		outData:              make(map[string][]map[string]interface{}),
	}, nil
}

// initChannels 初始化所有节点的监听通道（远程节点用信号通道，本地节点用缓冲通道）并返回排序后的通道名称数组
func (o *orchestrator) initChannels() []string {
	// 特殊处理，根节点（root）需要监听远程信号节点
	o.signalChan["root"] = tempWorkflow.GetSignalChannel(o.ctx, "root")
	for _, flowNode := range o.graph.GetAllNodes() {
		if flowNode.IsRemote { // 远程节点也需要同时拥有信号监听节点和本地队列节点
			o.signalChan[flowNode.Name] = tempWorkflow.GetSignalChannel(o.ctx, flowNode.Name)
			o.localChan[flowNode.Name] = tempWorkflow.NewNamedBufferedChannel(o.ctx, flowNode.Name, consts.DefaultChannelSize)
		} else { // 本地节点只需要本地队列节点
			localCh := tempWorkflow.NewNamedBufferedChannel(o.ctx, flowNode.Name, consts.DefaultChannelSize)
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
	logger := tempWorkflow.GetLogger(o.ctx)
	// 监听节点通道
	for _, signalName := range signalArr {
		nodeName := signalName // 向前兼容
		o.selector.AddReceive(o.signalChan[nodeName], func(c tempWorkflow.ReceiveChannel, more bool) {
			err := o.signalRecv(c, more, nodeName)
			if err != nil {
				logger.Error("信号逻辑执行异常", "error", err.Error())
				return
			}
		})
	}
}

// 节点信号监听监听
func (o *orchestrator) signalRecv(c tempWorkflow.ReceiveChannel, more bool, nodeName string) error {
	logger := tempWorkflow.GetLogger(o.ctx)
	if !more {
		return errors.New(fmt.Sprintf("信号通道已关闭,nodeName:%s", nodeName))
	}
	o.inflightSignals++
	defer func() { o.inflightSignals-- }()
	// 输入信号
	var signalInput *notify.SignalData
	c.Receive(o.ctx, &signalInput)
	if err := signalInput.Validate(); err != nil {
		return errors.New("信号数据解析失败:" + err.Error())
	}
	// 重复信号数据过滤
	if len(signalInput.DataId) > 0 {
		atyExecIds := o.activityExecUnqIds[signalInput.ActivityExecId]
		repeatIds := util.InArraysRepeat(atyExecIds, signalInput.DataId)
		if len(repeatIds) == len(signalInput.DataId) {
			return errors.New("接收到重复信号")
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
	// 存储上一个节点的执行结果
	if arrayutil.InArray(signalInput.NodeName, o.outNodes) {
		o.outData[signalInput.NodeName] = append(o.outData[signalInput.NodeName], signalInput.Data...)
	}
	// 获取当前节点
	logger.Info("收到信号", "signal", signalInput, "nextBranch", signalInput.Metadata.NextBranch, "traceId", signalInput.Metadata.TraceId)
	currentNode, exits := o.graph.GetNodeByName(signalInput.NodeName)
	if !exits {
		return fmt.Errorf("找不到下一个执行节点的定义: %s", signalInput.NodeName)
	}
	// 结束节点执行完毕之后，没必要进行后续流程执行
	if currentNode.ID == o.graph.EndNode.ID {
		return nil
	}
	// 获取待执行节点
	nodes, err := o.graph.GetNextNodes(currentNode.Name, signalInput.Metadata.NextBranch)
	if err != nil {
		return fmt.Errorf("获取next节点失败, error:%v", err)
	}
	logger.Info(currentNode.Name+" -> 下一批待执行节点", "step", o.stepCount, "nodes", nodes, "currentNode.Name", currentNode.Name)
	//signalInput.Data = nil
	if len(nodes) < 1 {
		return errors.New("待执行节点为空，请检查连接关系配置")
	}
	// 顺序执行每一个节点
	for _, nd := range nodes {
		// 解析节点的inputData
		branchInput, err := o.analysisParamInput(nd, signalInput.Data)
		if err != nil {
			return fmt.Errorf("解析节点的inputData失败:%v", err)
		}
		if len(branchInput) < 1 {
			logger.Debug("没有拿到上一个节点的执行结果")
		}
		o.enqueueOrSchedule(nd, branchInput, signalInput, o.handleActivity)
	}
	return nil
}

// 根据ParametersSource设置节点inputData的值
func (o *orchestrator) analysisParamInput(nd nodepkg.WkFLowNode, signalData []map[string]interface{}) ([]map[string]interface{}, error) {
	var branchInput = make([]map[string]interface{}, 0)
	switch {
	// 获取变量节点的值
	case strings.HasPrefix(nd.ParametersSource, "$var."):
		d, ok := o.express.GetWorkflowContext().GetNodeDataKV(nodepkg.ExpressVariablesNodeName, strings.TrimPrefix(nd.ParametersSource, "$var."))
		if ok {
			v, o := d.([]map[string]interface{})
			if o {
				branchInput = util.DeepCopy[string, interface{}](v)
			}
		}
		return branchInput, nil
	// 获取开始节点的值
	case strings.HasPrefix(nd.ParametersSource, "$global."):
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
		return branchInput, nil
	// 获取指定节点的响应
	case strings.HasPrefix(nd.ParametersSource, "$(") && strings.Contains(nd.ParametersSource, ")"):
		// 从执行节点中获取数据
		actRes := o.execNodeFinalResults[nd.ParametersSource]
		if actRes == nil || len(actRes.Data) == 0 {
			return nil, fmt.Errorf("未获取到指定节点(%s)的值", nd.ParametersSource)
		}
		// 获取值路径
		start := strings.Index(nd.ParametersSource, "$(")
		if start == -1 {
			return nil, errors.New("获取指定节点的响应异常1")
		}
		end := strings.Index(nd.ParametersSource, ")")
		if end == -1 {
			return nil, errors.New("获取指定节点的响应异常2")
		}
		pSource := strings.Trim(strings.TrimSpace(nd.ParametersSource[start+end+1:]), ".")
		// 解开data，递归取值
		for _, data := range actRes.Data {
			mVal := maputil.GetMapValue[string, interface{}](data, pSource, map[string]interface{}{})
			if v, ok := mVal.(map[string]interface{}); !ok {
				return nil, fmt.Errorf("值类型不是map[string]interface{}")
			} else {
				branchInput = append(branchInput, v)
			}
		}
		return branchInput, nil
	// 返回上一个节点中某一个的值
	case strings.HasPrefix(nd.ParametersSource, "$."):
		pSource := strings.TrimPrefix(nd.ParametersSource, "$.")
		for _, data := range signalData {
			mVal := maputil.GetMapValue[string, interface{}](data, pSource, map[string]interface{}{})
			if mv, err := cast.ToStringMapE(mVal); err != nil {
				sv, err := cast.ToSliceE(mVal)
				if err != nil {
					return nil, fmt.Errorf("值类型不是map[string]interface{}")
				}
				for _, v := range sv {
					smv, err := cast.ToStringMapE(v)
					if err != nil {
						return nil, fmt.Errorf("值类型不是map[string]interface{}2")
					}
					branchInput = append(branchInput, smv)
				}
				return branchInput, nil
			} else {
				branchInput = append(branchInput, mv)
			}
		}
		return branchInput, nil

	// 默认返回上个节点信号的值
	default:
		return signalData, nil
	}
}

// 任务调度（阻塞式窗口控制）当窗口已满时，直接阻塞等待空闲出现，避免内存膨胀；否则立刻调度执行
func (o *orchestrator) enqueueOrSchedule(n nodepkg.WkFLowNode, branchInput []map[string]interface{}, signalInput *notify.SignalData, fn scheduleNode) {
	// 执行超过最大步数则需要停止（设置了才有效，非环可以取消这个值）
	if o.maxStep > 0 && o.stepCount >= o.maxStep {
		tempWorkflow.GetLogger(o.ctx).Warn("节点已执行到最大步数上限，停止执行后续逻辑")
		return
	}
	// 若窗口已满，阻塞等待空闲（不入队，避免队列积压导致内存扩大）
	if o.registeredFutures >= o.selectorMaxFutures {
		_ = tempWorkflow.Await(o.ctx, func() bool { return o.registeredFutures < o.selectorMaxFutures })
	}
	o.totalTask += 1
	o.stepCount += 1
	fn(n, branchInput, signalInput)
	//o.handleActivity(n, branchInput, signalInput)
}

// 统一调度逻辑执行方法
type scheduleNode func(n nodepkg.WkFLowNode, branchInput []map[string]interface{}, signalInput *notify.SignalData)

// 子工作流处理方法
func (o *orchestrator) handleChildWorkflow(n nodepkg.WkFLowNode, branchInput []map[string]interface{}, signalInput *notify.SignalData) {
	// 执行同时批量执行branchInput
	tempWorkflowId := tempWorkflow.GetInfo(o.ctx).WorkflowExecution.ID
	for _, input := range branchInput {
		// 配置子工作流
		childWorkflowOptions := tempWorkflow.ChildWorkflowOptions{
			WorkflowID:               fmt.Sprintf("%s_%s_%s", tempWorkflowId, n.Name, util.UUID()),
			WorkflowExecutionTimeout: time.Hour * 24,
			ParentClosePolicy:        enums.PARENT_CLOSE_POLICY_TERMINATE, // 父工作流关闭时终止子工作流
		}
		childCtx := tempWorkflow.WithChildOptions(o.ctx, childWorkflowOptions)
		// 子工作流启动参数
		var genericWkFlow GenericWorkflowInput
		iptData, err := sonic.Marshal(input)
		if err != nil {
			continue
		}
		err = sonic.Unmarshal(iptData, &genericWkFlow)
		if err != nil {
			return
		}
		childFuture := tempWorkflow.ExecuteChildWorkflow(childCtx, GenericWorkflowWithMaxStep, genericWkFlow)
		o.registeredFutures += 1
		// 注册结果回调
		o.selector.AddFuture(childFuture, func(f tempWorkflow.Future) {
			defer func() {
				o.registeredFutures -= 1
				o.totalTask -= 1
			}()
			var childResult = map[string]interface{}{}
			err := f.Get(o.ctx, &childResult)
			if err != nil {
				tempWorkflow.GetLogger(o.ctx).Error("子工作流执行失败", "error", err)
				return
			}
			var atyExecId string
			sideEff := tempWorkflow.SideEffect(o.ctx, func(ctx tempWorkflow.Context) interface{} {
				return fmt.Sprintf("child_%s", util.UUID())
			})
			err = sideEff.Get(&atyExecId)
			if err != nil {
				tempWorkflow.GetLogger(o.ctx).Error("子工作流出转换UUID出错", "error", err)
				return
			}
			// 将工作流结果发送到对应的信号节点通道中
			o.localChan[n.Name].Send(o.ctx, notify.SignalData{
				ActivityExecId: atyExecId,
				NodeName:       n.Name,
				Data:           []map[string]interface{}{childResult},
				DataId:         nil,
				Metadata:       signalInput.Metadata,
			})
		})
	}
}

// handleActivity 调用Activity节点，并为其 Future 注册非阻塞结果回调
func (o *orchestrator) handleActivity(n nodepkg.WkFLowNode, branchInput []map[string]interface{}, signalInput *notify.SignalData) {
	logger := tempWorkflow.GetLogger(o.ctx)
	// activity执行配置
	activityOptions := tempWorkflow.ActivityOptions{
		StartToCloseTimeout: time.Hour * 1,
		HeartbeatTimeout:    time.Second * 30,
		RetryPolicy: &temporal.RetryPolicy{
			MaximumAttempts: 3,
		},
	}
	actCtx := tempWorkflow.WithActivityOptions(o.ctx, activityOptions)
	// 获取节点唯一执行ID
	var execID string
	rand := tempWorkflow.SideEffect(actCtx, func(ctx tempWorkflow.Context) interface{} {
		return fmt.Sprintf("acty_%s_%s", util.UUID(), time.Now().Format("20060102150405"))
	})
	_ = rand.Get(&execID)
	wkInfo := tempWorkflow.GetInfo(actCtx)
	// 节点入参
	activityInput := &nodepkg.ActivityInput{
		ExecID:      execID,
		InputData:   branchInput,
		SignalInput: signalInput,
		WorkflowID:  wkInfo.WorkflowExecution.ID,
		ExecutionID: wkInfo.WorkflowExecution.RunID,
		Express:     o.express,
		Node:        &n,
	}
	var execNode interface{}
	switch {
	case strings.HasSuffix(n.Type, ".ability_schedule"):
		execNode = "AbilitySchedule"
	case strings.HasSuffix(n.Type, ".start"):
		execNode = "Start"
	case strings.HasSuffix(n.Type, ".variable"):
		execNode = "Variable"
	case strings.HasSuffix(n.Type, ".pythonDocker") || strings.HasSuffix(n.Type, ".code"):
		execNode = "Code"
	case strings.HasSuffix(n.Type, ".condition") || strings.HasSuffix(n.Type, ".conditional"):
		execNode = "Conditional"
	case strings.HasSuffix(n.Type, ".sop"):
		execNode = "SOP"
	case strings.HasSuffix(n.Type, ".end"):
		execNode = "End"
	default:
		logger.Error("不支持的节点类型", "nodeType", n.Type, "nodeName", n.Name, "nodeId", n.ID)
		return
	}
	fut := tempWorkflow.ExecuteActivity(actCtx, execNode, activityInput)
	o.registeredFutures += 1
	nn := n
	exec := execID
	o.selector.AddFuture(fut, func(f tempWorkflow.Future) {
		var res *ActivityExecResult
		err := f.Get(o.ctx, &res)
		if err != nil {
			// 节点执行失败，也应该能够暂停流水线
			logger.Error("节点执行失败", "nodeName", n.Name, "error", err.Error())
			o.registeredFutures -= 1
			o.totalTask -= 1
			return
		}
		// 节点后置处理逻辑
		o.onNodeDone(nn, res, exec, signalInput)
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
		o.handleActivity(q.node, q.execInput.InputData, q.execInput.SignalInput)
	}
}

// onNodeDone 统一处理节点结果
func (o *orchestrator) onNodeDone(n nodepkg.WkFLowNode, res *ActivityExecResult, execID string, signalInput *notify.SignalData) {
	if res == nil {
		return
	}
	// 当节点执行完毕，删除这个节点的临时存储的taskUnqId列表
	delete(o.activityExecUnqIds, execID)
	// 开始批量处理 todo 确定下这个逻辑是否正确，理论上会将所有的批次响应拆解成单个信号（本地节点）
	for _, data := range res.Data {
		// 默认下个节点的分支为main，特殊节点后置处理
		res.Metadata.NextBranch = "main"
		// - 分支元数据更新（conditional）
		if strings.Contains(n.Type, ".conditional") {
			matchBranch := maputil.GetMapValue(data, "outputPaths", nil)
			if branch, ok := matchBranch.(string); ok {
				res.Metadata.NextBranch = branch
			}
		}
		// - 变量上下文写入（variable.set）
		if strings.Contains(n.Type, ".variable") && n.Parameters["operation"] == "set" && len(data) > 0 {
			o.express.GetWorkflowContext().SetNodeData(nodepkg.ExpressVariablesNodeName, data)
		}
		// 开始节点写入
		if strings.Contains(n.Type, ".start") && len(data) > 0 {
			o.express.GetWorkflowContext().SetNodeData(nodepkg.ExpressGlobalNodeName, data)
		}
		// 如果是sop节点，执行子工作流
		if strings.Contains(n.Type, ".sop") && len(data) > 0 {
			o.enqueueOrSchedule(n, res.Data, signalInput, o.handleChildWorkflow)
		}
		// 存储结果
		o.execNodeFinalResults[n.Name] = res
		// 非远程节点本地信号透传
		if !n.IsRemote {
			o.localChan[n.Name].Send(o.ctx, &notify.SignalData{
				ActivityExecId: execID,
				NodeName:       n.Name,
				Data:           []map[string]interface{}{data},
				Metadata:       res.Metadata,
			})
		}
		// 将节点结果写入到express中，这是为了让节点可以获取到指定节点的结果。但是要注意跳过远程节点，他们的结果不能存在这里（远程节点不能被提取结果）
		if res.Success && len(data) > 0 && !n.IsRemote {
			o.express.GetWorkflowContext().SetNodeData(n.Name, data)
		}
	}
}

// ContinuationState 续传状态
type ContinuationState struct {
	WorkflowContext      map[string]interface{}              // 工作流上下文
	ExecNodeFinalResults map[string]*ActivityExecResult      // 已执行节点的最终结果（节点ID+执行结果）
	ActivityExecUnqIds   map[string][]string                 // 已执行节点的去重集合（节点ID+执行ID）
	StepCount            int64                               // 当前已执行步数
	OutData              map[string][]map[string]interface{} // 节点输出结果暂存
}

// GenericWorkflowInput 通用工作流输入参数
type GenericWorkflowInput struct {
	WorkflowJSON             string                 `json:"workflow_json"`                // 工作流 JSON 定义
	InitialData              map[string]interface{} `json:"initial_data"`                 // 初始化数据
	MaxStep                  int64                  `json:"max_step"`                     // 最大步数，无环可设置为0，有环则必须设置
	Continuation             *ContinuationState     `json:"continuation"`                 // 续跑快照（上下文、执行结果、去重集合、步数）
	ContinueAsNewSelectTicks int                    `json:"continue_as_new_select_ticks"` // 续传阈值，超过该值触发 continue-as-new
}

// GenericWorkflowOutput 通用工作流输出参数
type GenericWorkflowOutput struct {
	WorkflowId string                 `json:"tempWorkflow_id"` // 工作流ID
	Data       map[string]interface{} `json:"data"`            // 执行结果
	OutMode    string                 `json:"out_mode"`        // 响应模式，目前仅memory
}

// GenericWorkflowWithMaxStep 带最大步数限制的通用 Temporal 工作流定义
// 要点：
// - 采用单 selector 统一接收信号与 Future 回调，保证非阻塞接收
// - 当窗口满时阻塞等待空闲，避免队列积压导致内存扩大
// - 退出条件：totalTask==0 且已无注册 Future（支持流式输出场景）
// - continue-as-new：基于 selectTicks 阈值触发以防历史事件过多
func GenericWorkflowWithMaxStep(ctx tempWorkflow.Context, in GenericWorkflowInput) (map[string][]map[string]interface{}, error) {
	// 设置全局变量存储节点
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
	logger := tempWorkflow.GetLogger(ctx)
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
	childCtx, cancelFunc := tempWorkflow.WithCancel(ctx)
	defer cancelFunc()
	o, err := newOrchestrator(childCtx, graph, express, in)
	if err != nil {
		return nil, err
	}
	// 恢复上下文，continue-as-new触发
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
		if in.Continuation.OutData != nil {
			o.outData = in.Continuation.OutData
		}
	}
	signalArr := o.initChannels()
	logger.Info("信号数组", "signalArr", signalArr)
	tempWorkflow.Go(childCtx, func(gCtx tempWorkflow.Context) {
		o.selector = tempWorkflow.NewSelector(gCtx)
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
				NextBranch: "main",
				TraceId:    tempWorkflow.GetInfo(ctx).WorkflowExecution.ID,
				Label:      "",
			},
		})
	}
	logger.Info("初始异步发送信号完成")
	if in.ContinueAsNewSelectTicks > 0 {
		o.continueThreshold = in.ContinueAsNewSelectTicks
	}

	// 流水线退出逻辑判定
	err = tempWorkflow.Await(childCtx, func() bool {
		return ((o.totalTask == 0 && o.registeredFutures == 0 && o.inflightSignals == 0) && o.selectTicks > 0) || o.continueRequested || (o.maxStep > 0 && o.stepCount >= o.maxStep)
	})
	if err != nil {
		return nil, err
	}

	// 触发continue-as-new
	if o.continueRequested {
		logger.Info("触发 continue-as-new 以控制历史事件规模", "selectTicks", o.selectTicks)
		state := &ContinuationState{
			WorkflowContext:      express.GetWorkflowContext().GetAllContext(),
			ExecNodeFinalResults: o.execNodeFinalResults,
			ActivityExecUnqIds:   o.activityExecUnqIds,
			StepCount:            o.stepCount,
			OutData:              o.outData,
		}
		nextIn := GenericWorkflowInput{
			WorkflowJSON:             in.WorkflowJSON,
			InitialData:              in.InitialData,
			MaxStep:                  o.maxStep,
			Continuation:             state,
			ContinueAsNewSelectTicks: o.continueThreshold,
		}
		return nil, tempWorkflow.NewContinueAsNewError(childCtx, GenericWorkflowWithMaxStep, nextIn)
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
		finalResult["maxStep"] = o.maxStep
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
			finalResult["tempWorkflowId"] = tempWorkflow.GetInfo(ctx).WorkflowExecution.ID // 从第一个节点获取工作流ID信息
			finalResult["tempWorkflowName"] = graph.Workflow.Name
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
	return o.outData, nil
}
