# 全球网测协议

主命令和 `speedtest` Go 包使用CAICT控制接口与测速节点。终端进度由原任务渲染器展示，传输字节、采样速率与回调由 `DataManager` 管理。

## 节点发现

控制服务默认为 `https://dlcv2.cnspeedtest.cn:8443`，可通过 `UserConfig.ControlURL` 或 `--control-url` 设置。

```text
GET /dataServer/getIpLocSP.php?tag=ios&mac={device}
GET /dataServer/mobilematch_many.php?ip=...&network=4&province=...
    &city=...&wifioper=...&mobileoperid=...&ipv6=0&model=iphone
    &pkg=com.cnspeedtest.globalspeed
```

IP 信息返回 `|` 分隔的出口 IP、地区数组和运营商。节点列表返回 JSON，包含 hostid、hostname、hostip、port、pname、city 等字段。客户端校验地址和端口，保留节点编号用于 `--server`。

实时发现失败或没有返回节点时使用内置节点表。取消请求会结束发现流程。内置表包含 App 包中解出的 722 条记录，其原始格式为 hex 编码 DES-ECB，密钥 `dw!@#$%^`。

## 会话

每轮传输阶段使用节点签发的 UUID。设备标识本地生成，格式为 `TS` 加 16 位大写十六进制字符。

```text
h1 = md5("model=" + model + "&imei=" + device)
h2 = md5("stime=" + unixSeconds + "&band=" + bandwidth + "&rand=12345555")
token = md5(h1 + h2)

GET /speed/dovalid?key=&flag=true&bandwidth={bandwidth}&model=iphone
    &imei={device}&time={unixSeconds}&token={token}
    &pkg=com.cnspeedtest.globalspeed
```

响应必须为 `1-{UUID}` 才视为签发成功。传输结束或取消后，使用独立的短超时请求释放会话：

```text
POST /speed/dovalid?key={UUID}
```

`-1-` 表示节点拒绝请求；响应本身没有给出具体拒绝原因。节点拒绝、HTTP 错误和响应格式错误会作为测速错误返回。

## 传输与统计

下载使用：

```text
GET /speed/File(1G).dl?r={签发时的 unixSeconds}&key={UUID}
```

`r` 取签发 dovalid 时使用的时间戳。客户端检查响应状态，通过 HTTP 正文统计字节，并在测量结束时取消读取。

节点按出口 IP 维护会话状态。若某节点此前收到过畸形请求，它会对该出口 IP 持续以 dovalid `-1-` 或下载 403 回应，直到状态过期或出口 IP 变化。换用其他节点或出口即可正常签发与下载。

上传使用 `POST /speed/doAnalsLoad.do`，请求头 `Key` 携带 UUID。正文为 multipart，boundary 为 `00content0boundary00`，文件字段为 `upload`，文件名以 `SPEED_` 开头。每个请求发送有明确长度的正文；成功收到服务器响应后，将该请求的载荷字节计入确认上传量。

`DataManager` 负责并发、测量时限、采样、回调与总字节统计。公开速率字段采用字节/秒。`Confirmed` 是成功上传请求的载荷字节占已发送载荷的比例。

每次独立调用 `DownloadTestContext` 或 `UploadTestContext` 都负责自己的会话生命周期。多节点测量为参与节点分别申请和释放会话。

## 延迟

HTTP、TCP 与 ICMP 模式用于延迟探测。传输过程中可独立采集负载延迟。探测丢失统计来自这些请求的实际成功与失败次数。

服务端未提供坐标时，客户端保留地区信息，地理距离显示为未知。

## 本地验证

```bash
go test ./...
go test -race ./speedtest .
```

测试服务检查请求签名、会话签发与释放、传输状态、字节统计、回调、取消，以及主命令的 TUI、JSON 和 JSONL 输出。
