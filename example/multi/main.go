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
	available := servers.Available()
	if len(*available) == 0 {
		log.Fatal("no available servers")
	}
	targets := (*available)[:min(3, len(*available))]
	server := targets[0]
	checkError(server.MultiDownloadTestContext(ctx, targets))
	checkError(server.MultiUploadTestContext(ctx, targets))
	fmt.Printf("Download: %s, Upload: %s\n", server.DLSpeed, server.ULSpeed)
}

func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
