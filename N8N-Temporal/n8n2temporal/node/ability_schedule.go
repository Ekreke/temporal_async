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
)

var (
	token = "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" // todo 待移出去,或者改为使用方传递
)

// AbilitySchedule 通用能力调度节点 - worker注册
type AbilitySchedule struct {
	grpcCli taskv2.TaskManagerServiceClient // 调度链接
	tempCli client.Client                   // temporal cli 链接
}

// AbilityScheduleParameters 定义节点的Parameters格式
type AbilityScheduleParameters struct {
	PbFile     string `json:"pb_file"`     // PB文件,必传
	ReqMessage string `json:"req_message"` // 请求参数message,必传
	RspMessage string `json:"rsp_message"` // 响应参数message,必传
	TaskKind   string `json:"task_kind"`   // 对应调度的任务类型
}

// NewAbilitySchedule 创建新的域名解析节点
func NewAbilitySchedule(taskCli taskv2.TaskManagerServiceClient, tmCli client.Client) *AbilitySchedule {
	return &AbilitySchedule{
		grpcCli: taskCli,
		tempCli: tmCli,
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
	ab := NewAbilityScheduleActivity(a.grpcCli, a.tempCli, input)
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
	*BaseActivity
	parameters *AbilityScheduleParameters // 节点参数
	conv       *convert.JsonPB            // 参数转换入口
}

// NewAbilityScheduleActivity 初始化执行对象
func NewAbilityScheduleActivity(grpcCli taskv2.TaskManagerServiceClient, tempCli client.Client, input *ActivityInput) *AbilityScheduleActivity {
	var a = &AbilityScheduleActivity{}
	a.grpcCli = grpcCli
	a.tempCli = tempCli
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

	// TODO: 这里需要创建一个 redis 连接实例，需要从config中读取配置
	// 本地测试
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	defer rdb.Close()

	// 测试 Redis 连接
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("连接 Redis 失败: %w", err)
	}

	var (
		res = &ExecNodeFuncResult{ // 响应结果
			Data: make([]map[string]interface{}, 0, len(taskUnqIds)),
		}
	)

	// 用于去重
	remainingTaskIds := make(map[string]bool, len(taskUnqIds))
	for _, id := range taskUnqIds {
		remainingTaskIds[id] = true
	}

	// 生成topic
	topic := fmt.Sprintf("task_stream_%s_%s", info.WorkflowExecution.ID, info.ActivityID)

	// 建立流式连接，传递topic
	// gRPC Stream 会返回当前的全量/快照数据，后续增量数据通过Redis Stream推送，由于stream 侧是分批查询，可能会产生增量数据，和redis stream 中重复，需要去重
	// TODO: 这里需要生产方 创建topic
	grpcStream, err := a.grpcCli.ListTasksStream(ctx, &taskv2.ListTasksStreamRequest{
		UniqueIds: taskUnqIds,
		// TODO: 确认任务状态
		StatusList: []taskv2.TaskStatus{
			taskv2.TaskStatus_TASK_STATUS_COMPLETED,
			taskv2.TaskStatus_TASK_STATUS_DISCARDED,
			taskv2.TaskStatus_TASK_STATUS_CANCELLED,
		},
		Topic: &topic,
	})
	if err != nil {
		return nil, fmt.Errorf("调用 ListTasksStream 失败: %w", err)
	}

	// grpc stream & redis stream 异步消费 -> len(taskUnqIds)*2
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
		// 处理已有的结果
		select {
		case err := <-errorChan:
			return nil, err
		// 如果gRPC Stream 读取结束，证明全量查询已经查询完毕，处理结果
		case <-doneChan:
			for item := range resultChan {
				if remainingTaskIds[item.UniqueId] {
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
		streams, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{topic, lastID},
			Block:   500 * time.Millisecond,
		}).Result()

		if err != nil && err != redis.Nil {
			logger.Warn("Redis XRead Error", "error", err)
			return nil, err
		}

		if len(streams) > 0 {
			// 这里只有一个topic ，所以是streams[0]
			for _, msg := range streams[0].Messages {
				lastID = msg.ID

				// 解析 Redis 消息并转换为 ListTasksStreamResponseItem 格式放入 channel
				// 这样统一处理逻辑
				uniqueIDStr, ok := msg.Values["unique_id"].(string)
				if !ok || uniqueIDStr == "" {
					continue
				}

				// TODO: 获取结果数据，这里的数据应该是一个json?
				var resultData []byte
				if val, ok := msg.Values["result"].(string); ok {
					resultData = []byte(val)
				}

				if len(resultData) > 0 {
					if remainingTaskIds[uniqueIDStr] {
						var dataVal map[string]interface{}
						if err := sonic.Unmarshal(resultData, &dataVal); err == nil {
							res.Data = append(res.Data, dataVal)
						} else {
							logger.Warn("Redis Result Unmarshal Error", "uniqueId", uniqueIDStr, "error", err)
						}
						delete(remainingTaskIds, uniqueIDStr)
					}
					if len(remainingTaskIds) == 0 {
						done = true
					}
				}
			}
		}
	}
	logger.Info("任务完成", "uniqueId", "remaining", len(remainingTaskIds))
	// 清理 topic
	cmd := rdb.Del(ctx, topic)
	_, err = cmd.Result()
	if err != nil {
		return nil, err
	}
	logger.Info("清理 topic", "topic", topic)
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

	type StreamItem struct {
		UniqueID string                 `json:"unique_id"`
		Result   map[string]interface{} `json:"result"`
	}

	// TODO: 这里需要创建一个 redis 连接实例，需要从config中读取配置
	// 本地测试
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	defer rdb.Close()

	// 测试 Redis 连接
	if err := rdb.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("连接 Redis 失败: %w", err)
	}

	// 用于去重
	remainingTaskIds := make(map[string]struct{}, len(taskUnqIds))
	for _, id := range taskUnqIds {
		remainingTaskIds[id] = struct{}{}
	}

	// 生成topic
	topic := fmt.Sprintf("task_stream_%s_%s", info.WorkflowExecution.ID, info.ActivityID)

	// 建立流式连接，传递topic
	// gRPC Stream 会返回当前的全量/快照数据，后续增量数据通过Redis Stream推送，由于stream 侧是分批查询，可能会产生增量数据，和redis stream 中重复，需要去重
	grpcStream, err := a.grpcCli.ListTasksStream(ctx, &taskv2.ListTasksStreamRequest{
		UniqueIds: taskUnqIds,
		StatusList: []taskv2.TaskStatus{
			taskv2.TaskStatus_TASK_STATUS_COMPLETED,
			taskv2.TaskStatus_TASK_STATUS_DISCARDED,
			taskv2.TaskStatus_TASK_STATUS_CANCELLED,
		},
		Topic: &topic,
	})
	if err != nil {
		return nil, fmt.Errorf("调用 ListTasksStream 失败: %w", err)
	}

	// grpc stream & redis stream 异步消费 -> len(taskUnqIds)*2
	resultChan := make(chan *StreamItem, len(taskUnqIds)*2)
	errorChan := make(chan error, 2)

	go func() {
		defer func() {
			close(resultChan)
			close(errorChan)
		}()
		for {
			// 接收到eof ，证明全量查询已经查询完毕
			resp, err := grpcStream.Recv()
			if err != nil {
				if err == io.EOF {
					logger.Info("gRPC Stream 读取结束")
					return
				}
				logger.Warn("gRPC Stream 读取错误", "error", err)
				errorChan <- err
				return
			}
			// gRPC Stream 读取到数据
			for _, item := range resp.GetItems() {
				bytesData, err := a.conv.AnyPBToJSON(ctx, a.parameters.RspMessage, item.Result)
				if err != nil {
					logger.Warn("AnyPBToJSON Error", "uniqueId", item.UniqueId, "error", err)
				} else {
					var dataVal map[string]interface{}
					if err := sonic.Unmarshal(bytesData, &dataVal); err != nil {
						logger.Warn("Unmarshal Error", "uniqueId", item.UniqueId, "error", err)
					} else {
						res.Data = append(res.Data, dataVal)
						resultChan <- &StreamItem{
							UniqueID: item.UniqueId,
							Result:   dataVal,
						}
						delete(remainingTaskIds, item.UniqueId)
					}
				}
			}
		}
	}()
	// Redis Stream 从头开始读
	lastID := "0"

	healthTicker := time.NewTicker(5 * time.Second)
	defer healthTicker.Stop()
	done := false
	for !done {
		var tmpRes = make([]map[string]interface{}, 0, len(taskUnqIds)) // 存储结果数据
		var temResIds = make([]string, 0, len(taskUnqIds))              // 存储信号返回数据的结果id

		streams, err := rdb.XRead(ctx, &redis.XReadArgs{
			Streams: []string{topic, lastID},
			Block:   500 * time.Millisecond,
		}).Result()

		if err != nil && err != redis.Nil {
			logger.Warn("Redis XRead Error", "error", err)
			return nil, err
		}

		// redis 有数据
		if len(streams) > 0 {
			for _, msg := range streams[0].Messages {
				lastID = msg.ID

				// TODO: stream 生产方 数据结构
				uniqueIDStr, ok := msg.Values["unique_id"].(string)
				if !ok || uniqueIDStr == "" {
					continue
				}
				var resultData []byte
				if val, ok := msg.Values["result"].(string); ok {
					resultData = []byte(val)
				}

				if len(resultData) > 0 {
					if _, ok := remainingTaskIds[uniqueIDStr]; ok {
						var dataVal map[string]interface{}
						if err := sonic.Unmarshal(resultData, &dataVal); err == nil {
							// 总数据
							res.Data = append(res.Data, dataVal)
							resultChan <- &StreamItem{
								UniqueID: uniqueIDStr,
								Result:   dataVal,
							}
						} else {
							logger.Warn("Redis Result Unmarshal Error", "uniqueId", uniqueIDStr, "error", err)
						}
						delete(remainingTaskIds, uniqueIDStr)
					}
					if len(remainingTaskIds) == 0 {
						done = true
					}
				}
			}
		}
		if done {
			break
		}
		select {
		case err := <-errorChan:
			return nil, err
		case <-resultChan:
			for item := range resultChan {
				tmpRes = append(tmpRes, item.Result)
				temResIds = append(temResIds, item.UniqueID)
			}
			// 发送信号 - 重置下一跳的branch
			input.SignalInput.Metadata.NextBranch = "main"
			var signalData = notify.SignalData{
				ActivityExecId: input.ExecID,
				NodeName:       input.Node.Name,
				Data:           tmpRes,
				DataId:         temResIds,
				Metadata:       input.SignalInput.Metadata,
			}
			err = a.tempCli.SignalWorkflow(ctx, info.WorkflowExecution.ID, "", input.Node.Name, signalData)
			if err != nil {
				return nil, err
			}
			activity.RecordHeartbeat(ctx, taskUnqIds)
			if len(taskUnqIds) == 0 { // 没有剩余带查询任务了，并且一个结果都没有返回
				return res, nil
			}
		case <-healthTicker.C:
			remaining := len(remainingTaskIds)
			activity.RecordHeartbeat(ctx, remaining)
			if remaining == 0 {
				done = true
			}
		}
	}
	return res, nil
}
