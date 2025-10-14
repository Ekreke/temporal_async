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

	w := worker.New(c, string(consts.Worker1), worker.Options{})

	w.RegisterActivity(temporal_workflow.Greet2)

	err = w.Run(worker.InterruptCh())
	if err != nil {
		log.Fatalln("Unable to start worker", err)
	}
}
