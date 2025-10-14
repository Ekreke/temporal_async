package main

import (
	"log"
	"test1/temporal-workflow/consts"

	"test1/temporal-workflow"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	w := worker.New(c, string(consts.Worker2), worker.Options{})

	// w.RegisterWorkflow(my_example.SayHelloWorldWorkflow)
	// w.RegisterActivity(my_example.Greet)
	// w.RegisterActivity(my_example.Greet2)
	// w.RegisterActivity(my_example.DomainResolveTask)

	// 注册新的workerflow 和 activity
	w.RegisterWorkflow(temporal_workflow.FiveActivityWorkflow) // 注册新的Workflow
	w.RegisterActivity(temporal_workflow.ActivityA1)
	w.RegisterActivity(temporal_workflow.ActivityA2)
	w.RegisterActivity(temporal_workflow.ActivityA3)
	w.RegisterActivity(temporal_workflow.ActivityA4)
	w.RegisterActivity(temporal_workflow.ActivityA5)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
