# globalspeedtest-go

CAICT命令行客户端，使用泰尔测速节点测量延迟、下载和上传速率。终端界面实时更新速率与负载延迟，支持节点选择、多节点测速、代理、源地址绑定和 JSON 输出。

## 构建

需要 Go 1.26 或更新版本。

```bash
go build -o globalspeedtest-go .
./globalspeedtest-go
```

默认根据出口 IP 的地区和运营商匹配节点，每个方向最多测量 10 秒。测速会消耗实际网络流量。

## 常用命令

```bash
# 调整测量时长与并发数
./globalspeedtest-go --duration 15s --thread 8

# 查看节点，按编号选择
./globalspeedtest-go --list
./globalspeedtest-go --server 2104002

# 指定节点地址
./globalspeedtest-go --custom-url http://11.4.5.14:1919

# 选择地区、运营商或搜索节点
./globalspeedtest-go --province 北京 --operator 移动
./globalspeedtest-go --search 上海

# 测速方向
./globalspeedtest-go --no-upload
./globalspeedtest-go --no-download

# 多节点测速
./globalspeedtest-go --multi

# 代理和源地址
./globalspeedtest-go --proxy http://127.0.0.1:7890
./globalspeedtest-go --source 192.168.1.100

# 输出格式
./globalspeedtest-go --unix
./globalspeedtest-go --json > result.json
./globalspeedtest-go --jsonl > result.jsonl
./globalspeedtest-go --unit decimal-bytes
```

`--help` 列出全部参数。在交互终端中显示动态任务；重定向输出或使用 `--unix` 时，只输出已完成任务。JSON/JSONL 保留原有结果字段，速率数值的单位是字节/秒。

每个测速阶段会申请自己的会话 key，并在结束或取消时释放。设备标识默认本地生成，可通过 `--device` 指定固定值。服务地址可以通过 `--control-url` 设置，便于部署和本地测试。

自动选节点时，若某节点在传回数据之前失败（延迟探测、会话签发或请求被拒），会按延迟顺序切换到下一个节点，最多尝试 `--fallback` 个（默认 3）。节点已传回数据后的失败（例如上行信号差）直接报错，不再切换。显式指定 `--server`、`--custom-url` 或 `--multi` 时不切换，失败直接报错。

延迟支持 HTTP、TCP 和 ICMP 探测，使用 `--ping-mode` 选择。探测丢失统计表示延迟请求的失败比例；负载延迟来自传输期间的独立探测。节点未提供坐标时，结果不计算地理距离。

## Go API

模块路径为 `github.com/XiaoMengXinX/globalspeedtest-go`，导入包后可使用客户端和进度回调：

```go
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
    client := speedtest.New(speedtest.WithUserConfig(&speedtest.UserConfig{
        MaxConnections: 4,
        Province: "北京",
        Operator: "移动",
    }))
    client.SetCaptureTime(10 * time.Second)
    client.SetCallbackDownload(func(rate speedtest.ByteRate) {
        fmt.Printf("\rDownload: %s", rate)
    })

    servers, err := client.FetchServerListContext(ctx)
    if err != nil { log.Fatal(err) }
    targets, err := servers.FindServer(nil)
    if err != nil { log.Fatal(err) }
    server := targets[0]
    if err := server.PingTestContext(ctx, nil); err != nil { log.Fatal(err) }
    if err := server.DownloadTestContext(ctx); err != nil { log.Fatal(err) }
    if err := server.UploadTestContext(ctx); err != nil { log.Fatal(err) }
    fmt.Printf("\nLatency: %s, Download: %s, Upload: %s\n",
        server.Latency, server.DLSpeed, server.ULSpeed)
}
```

`WithDoer` 可传入自定义 HTTP 客户端。请求使用客户端配置的代理、源地址、超时和传输层。`DownloadTestContext`、`UploadTestContext` 及多节点版本支持取消；`Reset` 清理测量状态并保留配置与回调。

## 测试

```bash
go test ./...
go test -race ./speedtest .
go build ./...
```

协议与命令行测试使用本地控制服务和测速节点，覆盖签发、传输、释放、取消、进度回调和终端输出。

## 协议

[协议说明](docs/globalspeed.md)介绍接口与会话流程，[终端界面说明](docs/tui.md)介绍渲染方式。

测速协议还原自 `com.cnspeedtest.globalspeed` 4.4.9。项目基于 [showwin/speedtest-go](https://github.com/showwin/speedtest-go)，采用 [MIT License](LICENSE)。
