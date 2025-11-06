package main

import (
	taskv2 "github.acme.red/backendhub/idl/gen/go/mapper/task/v2"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	nodepkg "n8n2temporal/node"
	"n8n2temporal/workflow"
)

// Worker 实现
func main() {
	// 创建 Temporal 客户端
	c, err := client.Dial(client.Options{
		HostPort: "localhost:7233",
		// todo 注册自定义Logger:
	})
	if err != nil {
		log.Fatalln("无法创建 Temporal 客户端:", err)
	}
	defer c.Close()

	// 创建 Worker
	w := worker.New(c, "n8n-conversion-queue-new", worker.Options{})

	// 初始化调度grpc链接
	taskServiceClient, err := grpc.NewClient("", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalln("初始化调度grpc链接失败:", err)
	}
	domainResolveNode := nodepkg.NewDomainResolveActivity(taskv2.NewTaskManagerServiceClient(taskServiceClient))

	// 注册工作流和活动
	w.RegisterWorkflow(workflow.GenericWorkflowWithMaxStep)

	// 注册所有活动

	// 注册自定义节点
	w.RegisterActivity(domainResolveNode.RegisterDomainResolve)
	//w.RegisterActivity(workflow.ExecuteDomainResolve)
	//w.RegisterActivity(workflow.ExecuteCustomNode)

	// 注册本地内置节点
	w.RegisterActivity(workflow.ExecuteStartNode)
	w.RegisterActivity(workflow.ExecuteEndNode)
	w.RegisterActivity(workflow.ExecuteVariableNode)
	w.RegisterActivity(workflow.ExecuteConditionalNode)
	w.RegisterActivity(workflow.ExecutePythonDockerNode)

	// 启动 Worker
	log.Println("启动 n8n 转换 Worker...")
	if err := w.Run(worker.InterruptCh()); err != nil {
		log.Fatalln("Worker 运行失败:", err)
	}
}
