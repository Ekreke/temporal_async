package main

import (
	"crypto/tls"
	taskv2 "github.acme.red/backendhub/idl/gen/go/mapper/task/v2"
	"go.temporal.io/sdk/client"
	tLog "go.temporal.io/sdk/log"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"log"
	"log/slog"
	nodepkg "n8n2temporal/node"
	"n8n2temporal/workflow"
	"os"
)

// Worker 实现
func main() {
	// 创建 Temporal 客户端
	c, err := client.Dial(client.Options{
		HostPort: "localhost:7233",
		Logger:   logger(),
	})
	if err != nil {
		log.Fatalln("无法创建 Temporal 客户端:", err)
	}
	defer c.Close()

	// 创建 Worker
	w := worker.New(c, "n8n-conversion-queue-new", worker.Options{})

	// 初始化调度grpc链接
	taskServiceClient, err := grpc.NewClient("task-manager.i.insec.cc:443", grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	if err != nil {
		log.Fatalln("初始化调度grpc链接失败:", err)
	}

	// 注册工作流和活动
	w.RegisterWorkflow(workflow.GenericWorkflowWithMaxStep)

	// 自定义能力节点
	abilitySchedule := nodepkg.NewAbilitySchedule(taskv2.NewTaskManagerServiceClient(taskServiceClient), c)
	w.RegisterActivity(abilitySchedule.AbilitySchedule)

	// 注册本地内置节点
	w.RegisterActivity(nodepkg.NewStartNode().Start)
	w.RegisterActivity(nodepkg.NewEndNode().End)
	w.RegisterActivity(nodepkg.NewVariableNode().Variable)
	w.RegisterActivity(nodepkg.NewConditional().Conditional)
	w.RegisterActivity(nodepkg.NewCode().Code)

	// 域名解析节点
	//domainResolveNode := nodepkg.NewDomainResolveActivity(taskv2.NewTaskManagerServiceClient(taskServiceClient), c)
	//w.RegisterActivity(domainResolveNode.DomainResolveActivity)

	// 启动 Worker
	log.Println("启动通用 Workerflow...")
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("Worker 运行失败:", err)
	}
}

func logger() tLog.Logger {
	slogLogger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo, // 设置日志级别
	}))
	return tLog.NewStructuredLogger(slogLogger)
}
