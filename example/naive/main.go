package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/XiaoMengXinX/globalspeedtest-go/speedtest"
)

func main() {
	ctx := context.Background()
	client := speedtest.New()
	client.SetCaptureTime(10 * time.Second)
	servers, err := client.FetchServerListContext(ctx)
	checkError(err)
	targets, err := servers.FindServer(nil)
	checkError(err)
	for _, server := range targets {
		checkError(server.PingTestContext(ctx, nil))
		checkError(server.DownloadTestContext(ctx))
		checkError(server.UploadTestContext(ctx))
		fmt.Printf("Latency: %s, Download: %s, Upload: %s\n", server.Latency, server.DLSpeed, server.ULSpeed)
		client.Reset()
	}
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
