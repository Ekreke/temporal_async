// 开始执行流水线
package main

import (
	"context"
	"log"
	"os"
	"test1/temporal-workflow"
	"test1/temporal-workflow/consts"

	"go.temporal.io/sdk/client"
)

func main() {
	c, err := client.Dial(client.Options{})
	if err != nil {
		log.Fatalln("Unable to create client", err)
	}
	defer c.Close()

	options := client.StartWorkflowOptions{
		ID:        "greeting-workflow",
		TaskQueue: string(consts.Worker1),
	}
	we, err := c.ExecuteWorkflow(context.Background(), options, temporal_workflow.SayHelloWorldWorkflow, os.Args[1])
	if err != nil {
		log.Fatalln("Unable to execute workflow", err)
	}
	log.Println("Started workflow", "WorkflowID", we.GetID(), "RunID", we.GetRunID())

	var result string
	err = we.Get(context.Background(), &result)
	if err != nil {
		log.Fatalln("Unable get workflow result", err)
	}
	log.Println("Workflow result:", result)

}
