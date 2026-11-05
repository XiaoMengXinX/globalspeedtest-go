package main

import (
	"context"
	"fmt"
	"log"

	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest"
)

func main() {
	ctx := context.Background()
	client := speedtest.New()
	servers, err := client.FetchServerListContext(ctx)
	checkError(err)
	targets, err := servers.FindServer(nil)
	checkError(err)
	for _, server := range targets {
		checkError(server.PingTestContext(ctx, nil))
		fmt.Printf("%s: latency %s, probe %s\n", server.Name, server.Latency, server.PacketLoss.String())
	}
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
