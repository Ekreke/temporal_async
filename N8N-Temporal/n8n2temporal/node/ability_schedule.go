package node

import (
	"context"
	"crypto/md5"
	"fmt"
	"io"
	"n8n2temporal/notify"
	"n8n2temporal/pkg/convert"
	"strings"
	"time"

	taskv2 "github.acme.red/backendhub/idl/gen/go/mapper/task/v2"
	"github.com/bytedance/sonic"
	"github.com/redis/go-redis/v9"
	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/log"
	"golang.org/x/sync/errgroup"
)

var (
	token = "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" // todo 待移出去,或者改为使用方传递
)

const (
	DefaultBatchSize = 100
	DefaultTimeout   = 5 * time.Second
)

// AbilitySchedule 通用能力调度节点 - worker注册
type AbilitySchedule struct {
	grpcCli taskv2.TaskManagerServiceClient // 调度链接
	tempCli client.Client                   // temporal cli 链接
	rdb     *redis.Client
}

// AbilityScheduleParameters 定义节点的Parameters格式
type AbilityScheduleParameters struct {
	PbFile     string `json:"pb_file"`     // PB文件,必传
	ReqMessage string `json:"req_message"` // 请求参数message,必传
	RspMessage string `json:"rsp_message"` // 响应参数message,必传
	TaskKind   string `json:"task_kind"`   // 对应调度的任务类型
}

// NewAbilitySchedule 创建新的能力调度节点
func NewAbilitySchedule(taskCli taskv2.TaskManagerServiceClient, tmCli client.Client, rdb *redis.Client) *AbilitySchedule {
	return &AbilitySchedule{
		grpcCli: taskCli,
		tempCli: tmCli,
		rdb:     rdb,
	}
}

// AbilitySchedule 注册节点方法,将作为节点的执行入口(注意并发执行问题)
func (a *AbilitySchedule) AbilitySchedule(ctx context.Context, input *ActivityInput) (*ActivityOutput, error) {
	if a.grpcCli == nil {
		return nil, fmt.Errorf("AbilitySchedule Error, grpc client not initialized")
	}
	if a.tempCli == nil {
		return nil, fmt.Errorf("AbilitySchedule Error, temporal client not initialized")
	}
	ab := NewAbilityScheduleActivity(a.grpcCli, a.tempCli, input, a.rdb)
	// 验证参数，补全结构体数据
	if err := ab.ValidateInput(input); err != nil {
		return nil, err
	}
	return ab.ExecuteWithExecuteTiming(ctx, input, ab.executeDomainResolve)
}

// 能力调度节点（通用）
var _ Activity = (*AbilityScheduleActivity)(nil)

// AbilityScheduleActivity 实际节点执行对象
type AbilityScheduleActivity struct {
	grpcCli taskv2.TaskManagerServiceClient // 调度链接
	tempCli client.Client                   // temporal cli 链接
	rdb     *redis.Client                   // redis 链接
	*BaseActivity
	parameters *AbilityScheduleParameters // 节点参数
	conv       *convert.JsonPB            // 参数转换入口
}

// StreamItem 流式输出的结果项
type StreamItem struct {
	UniqueID string                 `json:"unique_id"`
	Result   map[string]interface{} `json:"result"`
}

// SignalBatcher 负责缓冲数据、定时/定量发送信号
type SignalBatcher struct {
	ctx    context.Context
	cancel context.CancelFunc
	// 消息聚合chan
	aggChan <-chan *StreamItem

	remainingTaskIds map[string]struct{}

	// 实际发送信号的回调函数
	signalFunc func(ctx context.Context,
		data []map[string]interface{},
		ids []string,
		input *ActivityInput,
		info activity.Info) error
	// 缓冲数据
	buffer   []map[string]interface{}
	idBuffer []string
	// 控制参数
	batchSize      int          // 达到此数量立即发送 (例如 100)
	ticker         *time.Ticker // 定时器，用于超时发送 (例如 5 秒)
	tickerDuration time.Duration
	// 节点信息
	input *ActivityInput
	info  activity.Info
}

// NewSignalBatcher 信号发送器
func NewSignalBatcher(ctx context.Context, aggChan <-chan *StreamItem,
	signalFunc func(ctx context.Context,
		data []map[string]interface{},
		ids []string,
		input *ActivityInput,
		info activity.Info) error,
	input *ActivityInput,
	info activity.Info) *SignalBatcher {
	ctx, cancel := context.WithCancel(ctx)
	return &SignalBatcher{
		ctx:            ctx,
		cancel:         cancel,
		aggChan:        aggChan,
		signalFunc:     signalFunc,
		buffer:         make([]map[string]interface{}, 0),
		idBuffer:       make([]string, 0),
		batchSize:      DefaultBatchSize,
		tickerDuration: DefaultTimeout,
		input:          input,
		info:           info,
	}
}

// readBatch 尝试从 aggChan 中非阻塞地读取更多数据，直到达到 batchSize 或 Channel 暂时为空
func (s *SignalBatcher) readBatch() {
	for len(s.buffer) < s.batchSize {
		select {
		case item, ok := <-s.aggChan:
			if !ok {
				return
			}
			s.buffer = append(s.buffer, item.Result)
			s.idBuffer = append(s.idBuffer, item.UniqueID)
		default:
			return
		}
	}
}

// flushSignal 发送信号
func (s *SignalBatcher) flushSignal() error {
	// 检查是否有数据需要发送
	if len(s.buffer) == 0 {
		return nil
	}

	// 调用函数发送
	err := s.signalFunc(s.ctx, s.buffer, s.idBuffer, s.input, s.info)
	if err != nil {
		return fmt.Errorf("发送信号失败: %w", err)
	}

	// 清空缓冲区
	s.buffer = s.buffer[:0]
	s.idBuffer = s.idBuffer[:0]

	// 重置remainingTaskIds
	for _, id := range s.idBuffer {
		delete(s.remainingTaskIds, id)
	}
	return nil
}

// Stop 用于安全地停止 SignalBatcher 协程
func (s *SignalBatcher) Stop() {
	s.cancel()
}

// Run 启动 SignalBatcher 的核心处理循环
func (s *SignalBatcher) Run() error {
	s.ticker = time.NewTicker(s.tickerDuration)
	defer s.ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			// 发送存量数据
			if err := s.flushSignal(); err != nil {
				return err
			}
			// 发送心跳
			activity.RecordHeartbeat(s.ctx, len(s.remainingTaskIds))
			return nil

			// 非阻塞写入buffer & buffer 满发送
		case item, ok := <-s.aggChan:
			// 通道关闭 & 发送存量数据 & 发送心跳
			if !ok {
				if err := s.flushSignal(); err != nil {
					return err
				}
				activity.RecordHeartbeat(s.ctx, len(s.remainingTaskIds))
				return nil
			}

			s.buffer = append(s.buffer, item.Result)
			s.idBuffer = append(s.idBuffer, item.UniqueID)

			if len(s.buffer) < s.batchSize {
				s.readBatch()
			}

			if len(s.buffer) >= s.batchSize {
				if err := s.flushSignal(); err != nil {
					return err
				}
				activity.RecordHeartbeat(s.ctx, len(s.remainingTaskIds))
				s.ticker.Reset(s.tickerDuration)
			}

		// 到期批量发送数据，这里也是有可能发送超batchsize 的数据
		case <-s.ticker.C:
			if err := s.flushSignal(); err != nil {
				return err
			}
			activity.RecordHeartbeat(s.ctx, len(s.remainingTaskIds))
		}
	}
}

// sendSignal 信号发送函数
func (a *AbilityScheduleActivity) sendSignal(ctx context.Context, data []map[string]interface{}, ids []string, input *ActivityInput, info activity.Info) error {
	input.SignalInput.Metadata.NextBranch = "main"
	var signalData = notify.SignalData{
		ActivityExecId: input.ExecID,
		NodeName:       input.Node.Name,
		Data:           data,
		DataId:         ids,
		Metadata:       input.SignalInput.Metadata,
	}
	// ctx -> errgroup contxt
	err := a.tempCli.SignalWorkflow(ctx, info.WorkflowExecution.ID, "", input.Node.Name, signalData)
	if err != nil {
		return err
	}
	return nil
}

// NewAbilityScheduleActivity 初始化执行对象
func NewAbilityScheduleActivity(grpcCli taskv2.TaskManagerServiceClient, tempCli client.Client, input *ActivityInput, rdb *redis.Client) *AbilityScheduleActivity {
	var a = &AbilityScheduleActivity{}
	a.grpcCli = grpcCli
	a.tempCli = tempCli
	a.rdb = rdb
	a.BaseActivity = &BaseActivity{
		NodeInfo:            input.Node,
		expressionEvaluator: input.Express,
	}
	return a
}

// GetNodeInfo 获取当前节点信息
func (a *AbilityScheduleActivity) GetNodeInfo() *WkFLowNode {
	return a.NodeInfo
}

// ValidateInput 验证输入参数（实现NodeActivity接口）
func (a *AbilityScheduleActivity) ValidateInput(input *ActivityInput) error {
	// 基础验证
	if err := a.BaseActivity.ValidateInput(input); err != nil {
		return err
	}
	// 验证三个必穿参数
	params, err := sonic.Marshal(input.Node.Parameters)
	if err != nil {
		return err
	}
	var asParams = &AbilityScheduleParameters{}
	err = sonic.Unmarshal(params, &asParams)
	if err != nil {
		return err
	}
	if asParams.PbFile == "" || asParams.ReqMessage == "" || asParams.RspMessage == "" || asParams.TaskKind == "" {
		return fmt.Errorf("AbilitySchedule Params Error, pbFile or ReqMessage or TaskKind or RspMessage is empty")
	}
	a.parameters = asParams
	a.conv = convert.NewJsonPB(asParams.PbFile, token, nil)
	return nil
}

func (a *AbilityScheduleActivity) executeDomainResolve(ctx context.Context, input *ActivityInput) (*ExecNodeFuncResult, error) {
	var taskUnqIds = make([]string, 0)
	var heartBeat = make([]string, 0)
	if activity.HasHeartbeatDetails(ctx) {
		_ = activity.GetHeartbeatDetails(ctx, &heartBeat)
	}
	if len(heartBeat) > 0 { // 有心跳数据则直接获取即可
		taskUnqIds = heartBeat
	} else { // 无心跳数据说明则重新建任务执行
		taskIds, err := a.CreateScheduleTask(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("CreateScheduleTask Error: %v", err)
		}
		taskUnqIds = taskIds
	}
	// 获取调度结果（如果是阻塞，那么Data的数量和taskUnqIds相等；如果是流式，那么最终流式返回的时候，也和阻塞的数据式一样的）
	if !input.Node.IsRemote {
		return a.output(ctx, taskUnqIds)
	}
	return a.streamOutput(ctx, taskUnqIds, input)
}

// CreateScheduleTask 创建调度任务
func (a *AbilityScheduleActivity) CreateScheduleTask(ctx context.Context, input *ActivityInput) ([]string, error) {
	logger := a.GetLogger(ctx)
	// 元数据标签
	metaData, err := sonic.Marshal(input.SignalInput.Metadata)
	if err != nil {
		return nil, err
	}
	var labelMap = map[string]string{}
	err = sonic.Unmarshal(metaData, &labelMap)
	if err != nil {
		return nil, err
	}
	// 遍历所有响应数据，组装tasks请求
	tasks := make([]*taskv2.Task, 0, len(input.InputData))
	taskUnqIdMap := make(map[string]struct{})
	for _, inputData := range input.InputData {
		bytesData, err := sonic.Marshal(inputData)
		if err != nil {
			logger.Warn("CreateScheduleTask Marshal Error", "error", err)
			continue
		}
		args, err := a.conv.JSONToAnyPB(ctx, a.parameters.ReqMessage, bytesData)
		if err != nil {
			logger.Warn("CreateScheduleTask Convert PB Error", "error", err)
			logger.Debug("debug info data", "node", a.NodeInfo.Name, "ReqMessage", a.parameters.ReqMessage, "bytesData", string(bytesData))
			continue
		}
		// 请求参数构建
		task := &taskv2.Task{
			Kind: a.parameters.TaskKind, // 任务类型，对应POC、爬虫等枚举
			//ParentUniqueId: input.SignalData.NodeName,                            // 父ID，上一个节点的ID（信号节点接收到的就是上一个节点的执行结果）
			GroupId:        input.WorkflowID,    // 组ID用于区分流水线,当前正在运行的流水线RunID
			Args:           args,                // 参数，节点执行的参数
			Labels:         labelMap,            // 标签，透传
			WorkerSelector: map[string]string{}, // 工作节点选择，暂时为空
		}
		// 计算唯一ID
		var strBuilder strings.Builder
		strBuilder.WriteString(task.Kind)
		strBuilder.WriteString(task.GroupId)
		strBuilder.Write(task.Args.GetValue())
		task.UniqueId = fmt.Sprintf("%x", md5.Sum([]byte(strBuilder.String())))
		// 表示任务重复了
		if _, ok := taskUnqIdMap[task.UniqueId]; ok {
			logger.Warn("重复任务参数", "UniqueId", task.UniqueId)
			continue
		}
		taskUnqIdMap[task.UniqueId] = struct{}{}
		tasks = append(tasks, task)
	}
	if len(tasks) < 1 {
		return nil, fmt.Errorf("CreateScheduleTask Error, no tasks found")
	}
	// 统计任务id列表
	var taskUnqIds = make([]string, 0, len(tasks))
	for _, task := range tasks {
		taskUnqIds = append(taskUnqIds, task.UniqueId)
	}
	logger.Info("调度任务列表", "taskUnqIds", strings.Join(taskUnqIds, ","))
	// 发送到调度，执行任务（不要获取结果，因为结果表示的是任务的增量，但我们只关心创建和丢失的任务数量）
	_, err = a.grpcCli.CreateTasks(ctx, &taskv2.CreateTasksRequest{Tasks: tasks})
	if err != nil {
		return nil, err
	}
	return taskUnqIds, nil
}

// 阻塞输出 - 使用混合模式 (gRPC Stream + Redis Stream)
func (a *AbilityScheduleActivity) output(ctx context.Context, taskUnqIds []string) (*ExecNodeFuncResult, error) {
	logger := a.GetLogger(ctx)
	info := activity.GetInfo(ctx)
	var (
		res = &ExecNodeFuncResult{ // 响应结果
			Data: make([]map[string]interface{}, 0, len(taskUnqIds)),
		}
	)
	// 用于去重
	remainingTaskIds := make(map[string]struct{}, len(taskUnqIds))
	for _, id := range taskUnqIds {
		remainingTaskIds[id] = struct{}{}
	}

	// 生成topic
	topic := fmt.Sprintf("task_stream_%s_%s", info.WorkflowExecution.ID, info.ActivityID)

	defer func() {
		err := a.rdb.Del(ctx, topic).Err()
		if err != nil {
			logger.Warn("删除 redis stream 失败", "error", err)
		}
	}()

	grpcStream, err := a.grpcCli.ListTasksStream(ctx, &taskv2.ListTasksStreamRequest{
		UniqueIds: taskUnqIds,
		Topic:     &topic,
	})
	if err != nil {
		return nil, fmt.Errorf("调用 ListTasksStream 失败: %w", err)
	}

	resultChan := make(chan *taskv2.ListTasksStreamResponseItem, len(taskUnqIds)*2)
	errorChan := make(chan error, 2)
	doneChan := make(chan struct{})

	// 异步 消费 gRPC Stream
	go func() {
		defer func() {
			close(resultChan)
			close(doneChan)
			close(errorChan)
		}()

		for {
			// 接收到eof ，证明全量查询已经查询完毕
			resp, err := grpcStream.Recv()
			if err != nil {
				if err == io.EOF {
					logger.Info("gRPC Stream 读取结束")
					doneChan <- struct{}{}
					return
				}
				logger.Warn("gRPC Stream 读取错误", "error", err)
				doneChan <- struct{}{}
				errorChan <- err
				return
			}
			for _, item := range resp.GetItems() {
				resultChan <- item
			}
		}
	}()

	// Redis Stream 从头开始读
	lastID := "0"

	healthTicker := time.NewTicker(5 * time.Second)
	defer healthTicker.Stop()

	// 标记是否完成
	done := false

	for !done {
		select {
		case err := <-errorChan:
			return nil, err
		// 如果gRPC Stream 读取结束，证明全量查询已经查询完毕，处理结果
		case <-doneChan:
			for item := range resultChan {
				a.processStreamItem(ctx, item, res, remainingTaskIds, logger)
			}

			if len(remainingTaskIds) == 0 {
				done = true
			}
			continue // 继续处理 channel 中的数据
		case <-healthTicker.C:
			remaining := len(remainingTaskIds)
			activity.RecordHeartbeat(ctx, remaining)
			if remaining == 0 {
				done = true
			}
		}

		if done {
			break
		}

		// 阻塞读取 Redis Stream, 避免频繁访问 redis , 默认读取stream 中未被消费的全部数据
		streams, err := a.rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{topic, lastID},
			Block:   0,
		}).Result()

		if err != nil && err != redis.Nil {
			logger.Warn("Redis XRead Error", "error", err)
			return nil, err
		}

		if len(streams) > 0 {
			// 这里只有一个topic ，所以是streams[0]
			for _, msg := range streams[0].Messages {
				lastID = msg.ID
				if a.processRedisMessage(msg, res, remainingTaskIds, &done, logger) {
					break
				}
			}
		}
	}
	return res, nil
}

// 流式输出
func (a *AbilityScheduleActivity) streamOutput(ctx context.Context, taskUnqIds []string, input *ActivityInput) (*ExecNodeFuncResult, error) {
	var (
		res = &ExecNodeFuncResult{ // 最终响应结果
			Data: make([]map[string]interface{}, 0, len(taskUnqIds)),
		}
		logger = a.GetLogger(ctx)      // 日志信息
		info   = activity.GetInfo(ctx) // activity信息
	)
	// 数据聚合channel
	aggChan := make(chan *StreamItem, len(taskUnqIds)*2)

	defer func() {
		close(aggChan)
	}()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 用于去重
	remainingTaskIds := make(map[string]struct{}, len(taskUnqIds))
	for _, id := range taskUnqIds {
		remainingTaskIds[id] = struct{}{}
	}

	// 生成topic
	topic := fmt.Sprintf("task_stream_%s_%s", info.WorkflowExecution.ID, info.ActivityID)

	defer func() {
		if err := a.rdb.Del(ctx, topic).Err(); err != nil {
			logger.Warn("删除 redis stream 失败", "error", err)
		}
	}()

	// gRPC Stream 会返回当前的全量/快照数据，后续增量数据通过Redis Stream推送，由于stream 侧是分批查询，可能会产生增量数据，和redis stream 中重复，需要去重
	grpcStream, err := a.grpcCli.ListTasksStream(ctx, &taskv2.ListTasksStreamRequest{
		UniqueIds: taskUnqIds,
		Topic:     &topic,
	})
	if err != nil {
		return nil, fmt.Errorf("调用 ListTasksStream 失败: %w", err)
	}
	g, ctx := errgroup.WithContext(ctx)
	batcher := NewSignalBatcher(ctx, aggChan, a.sendSignal, input, info)
	batcher.remainingTaskIds = remainingTaskIds

	// 信号发送 goroutine
	g.Go(func() error {
		return batcher.Run()
	})

	// grpc stream 读取 groutine
	g.Go(func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				// 接收到eof ，证明全量查询已经查询完毕
				resp, err := grpcStream.Recv()
				if err != nil {
					if err == io.EOF {
						logger.Info("gRPC Stream 读取结束")
						return nil
					}
					logger.Warn("gRPC Stream 读取错误", "error", err)
					return err
				}
				// gRPC Stream 读取到数据
				for _, item := range resp.GetItems() {
					a.processGrpcStreamItem(ctx, item, aggChan, logger)
				}
			}

		}
	})

	// redis stream 读取goroutine
	g.Go(func() error {
		lastID := "0"
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				streams, err := a.rdb.XRead(ctx, &redis.XReadArgs{
					Streams: []string{topic, lastID},
					// 一直阻塞直到有新数据到达
					Block: 0,
				}).Result()

				if err != nil && err != redis.Nil {
					logger.Warn("Redis XRead Error", "error", err)
					return err
				}
				if len(streams) == 0 {
					continue
				}
				for _, msg := range streams[0].Messages {
					lastID = msg.ID
					// TODO: 这里要去除result，要重新调用一下Liststream
					a.processRedisStreamMessage(ctx, msg, aggChan, logger)
				}
			}
		}
	})

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return res, nil
}

// processGrpcStreamItem 处理gRPC Stream返回的单个数据项
func (a *AbilityScheduleActivity) processGrpcStreamItem(
	ctx context.Context,
	item *taskv2.ListTasksStreamResponseItem,
	aggResultChan chan *StreamItem,
	logger log.Logger,
) {
	// 将PB格式的结果转换为JSON
	bytesData, err := a.conv.AnyPBToJSON(ctx, a.parameters.RspMessage, item.Result)
	if err != nil {
		logger.Warn("AnyPBToJSON Error", "uniqueId", item.UniqueId, "error", err)
		return
	}

	// 反序列化JSON数据
	var dataVal map[string]interface{}
	if err := sonic.Unmarshal(bytesData, &dataVal); err != nil {
		logger.Warn("Unmarshal Error", "uniqueId", item.UniqueId, "error", err)
		return
	}

	select {
	case aggResultChan <- &StreamItem{
		UniqueID: item.UniqueId,
		Result:   dataVal,
	}:
	case <-ctx.Done():
		return
	}

}

func (a *AbilityScheduleActivity) processRedisStreamMessage(
	ctx context.Context,
	msg redis.XMessage,
	aggResultChan chan *StreamItem,
	logger log.Logger,
) {
	// 解析 unique_id
	uniqueIDStr, ok := msg.Values["unique_id"].(string)
	if !ok || uniqueIDStr == "" {
		return
	}

	// 解析 result 数据
	var resultData []byte
	if val, ok := msg.Values["result"].(string); ok {
		resultData = []byte(val)
	}

	if len(resultData) > 0 {
		var dataVal map[string]interface{}
		if err := sonic.Unmarshal(resultData, &dataVal); err == nil {
			// 发送到结果channel
			select {
			case aggResultChan <- &StreamItem{
				UniqueID: uniqueIDStr,
				Result:   dataVal,
			}:
			case <-ctx.Done():
				return
			}
		} else {
			logger.Warn("Redis Result Unmarshal Error", "uniqueId", uniqueIDStr, "error", err)
		}
	}
}

// processRedisMessage 处理 Redis 流消息
func (a *AbilityScheduleActivity) processRedisMessage(msg redis.XMessage, res *ExecNodeFuncResult, remainingTaskIds map[string]struct{}, done *bool, logger log.Logger) bool {
	// 解析 Redis 消息并转换为 ListTasksStreamResponseItem 格式放入 channel
	// 这样统一处理逻辑
	uniqueIDStr, ok := msg.Values["unique_id"].(string)
	if !ok || uniqueIDStr == "" {
		return false
	}

	var resultData []byte
	if val, ok := msg.Values["result"].(string); ok {
		resultData = []byte(val)
	}

	if len(resultData) > 0 {
		if _, ok := remainingTaskIds[uniqueIDStr]; ok {
			var dataVal map[string]interface{}
			if err := sonic.Unmarshal(resultData, &dataVal); err == nil {
				res.Data = append(res.Data, dataVal)
			} else {
				logger.Warn("Redis Result Unmarshal Error", "uniqueId", uniqueIDStr, "error", err)
			}
			delete(remainingTaskIds, uniqueIDStr)
		}
		if len(remainingTaskIds) == 0 {
			*done = true
			return true
		}
	}
	return false
}

// processStreamItem 处理单个流式数据项
func (a *AbilityScheduleActivity) processStreamItem(ctx context.Context, item *taskv2.ListTasksStreamResponseItem, res *ExecNodeFuncResult, remainingTaskIds map[string]struct{}, logger log.Logger) {
	if _, ok := remainingTaskIds[item.UniqueId]; ok {
		if item.Result != nil && len(item.Result.GetValue()) > 0 {
			bytesData, err := a.conv.AnyPBToJSON(ctx, a.parameters.RspMessage, item.Result)
			if err != nil {
				logger.Warn("AnyPBToJSON Error", "uniqueId", item.UniqueId, "error", err)
			} else {
				var dataVal map[string]interface{}
				if err := sonic.Unmarshal(bytesData, &dataVal); err != nil {
					logger.Warn("Unmarshal Error", "uniqueId", item.UniqueId, "error", err)
				} else {
					res.Data = append(res.Data, dataVal)
				}
			}
		}
		delete(remainingTaskIds, item.UniqueId)
		logger.Debug("任务完成", "uniqueId", item.UniqueId, "remaining", len(remainingTaskIds))
	}
}
