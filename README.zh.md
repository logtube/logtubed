# 如何部署 logtubed

## 编译 logtubed

1. 按照官方文档，安装 Go
2. 使用命令行，浏览到代码根目录
3. 编译 logtubed

   本代码仓库已经包含所有依赖库源代码，且 Go 支持跨平台编译，因此可以在 Windows 环境中编译出 Linux 可执行代码

    * 在 Linux 环境下执行以下命令

        ```shell
        go build -mod vendor
        ```

    * 在 Windows （PowerShell）环境中执行以下命令，可以直接构建出 Linux 可执行文件

        ```powershell
        $Env:GOOS="linux"
        $Env:CGO_ENABLED="0"
        go build -mod vendor
        ```

## 配置文件

Logtubed 处理流程为 "输入" -> "过滤" -> "队列" -> "输出"，配置项也围绕着这个流程展开

```yaml
# 关闭自身的调试日志
verbose: false

# Go 内置性能分析器
pprof:
  block: 1
  mutex: 1

# Redis 输入配置
input_redis:
  # 打开此功能
  enabled: true
  # 直接监听 6379，冒充 Redis 服务器
  bind: 0.0.0.0:6379
  # 控制冒充的 Redis 版本号，从而控制 Filebeat 是否在单条 LPUSH/RPUSH 命令中塞多条日志
  multi: false
  # 配置
  pipeline:
    logtube:
      # 最初版本的 Logtube 日志格式不包含时区信息，所以必须指定默认时区
      time_offset: -8
    mysql:
      error_ignore_levels:
        - note

# 自己开发的 SPTP UDP 协议，基本上没在使用
input_sptp:
  # 关闭此功能
  enabled: false
  # 监听地址
  bind: 0.0.0.0:9921

# 主题控制
topics:
  # 高优先级主题，拥有独立的队列
  priors:
    - err
    - x-access
  # info 主题必须包含 keyword 才会被收集，要不然他们瞎输出的 info 够日志系统喝一壶
  keyword_required:
    - info
  # trace 和 debug 日志被直接忽略
  ignored:
    - debug
    - trace

# 重命名环境名和主题，便于统一命名习惯
mappings:
  env:
    development: dev
    local: dev
    uat: staging
    production: prod
  topic:
    error: err

# 磁盘队列，Redis 协议接受的日志会先写入到磁盘，防止重启或者故障时日志丢失
queue:
  # 磁盘队列目录
  dir: /data/xlogd
  # 磁盘队列文件名
  name: xlogd
  # 每隔多少条日志进行一次持久化写入
  sync_every: 10000
  # 水位，磁盘队列超过 6GB 的时候，冒充的 Redis 服务器会对所用命令抛出错误，Filebeat 收到错误就会暂停写入并重连
  watermark: 6

# ES 输出
output_es:
  # 打开此功能
  enabled: true
  # 地址
  urls:
    - http://127.0.0.1:9200
  # 是否停用客户端嗅探功能，如果是容器部署的 ES 可以把这个选项设置为 true 避免连接不到服务器
  no_siff: false
  # 为了实现最大吞吐量，logtubed 会分批将日志写入 ES，逻辑为数量满足 batch_size 时，或者等待超过 batch_timeout 秒后，将该批次写入 ES
  # 每批次写入数量
  batch_size: 4000
  # 最大批次延迟，单位为秒
  batch_timeout: 3
  # 同时有多少批次写入
  concurrency: 16
  # ES 7.x 以上默认不再开启 Mapping Type 功能，可以把这个选项设置为 true，这时 logtubed 输出 ES 不再设置 Mapping Type
  no_mapping_types: false

# 把日志重新输出到本地文件
output_local:
  # 关闭此功能
  enabled: false
  dir: /var/log/logtube-logs
```

## 载入索引模板到 ES

ElasticSearch 模板定义在单独的代码仓库里面

https://github.com/logtube/elasticsearch-templates

使用如下 Docker 命令可以自动将模板载入到 ES

```shell
docker run --rm -e ES_URL=http://elasticsearch:9200 guoyk/logtube-esloadtpl
```

如果使用 ES 7.x 以上版本，并弃用 Mapping Type 功能，则使用以下命令载入无 Mapping Type 版本的索引模板

```shell
docker run --rm -e ES_URL=http://elasticsearch:9200 guoyk/logtube-esloadtpl:nomt
```

## 启动 Logtubed

```
./logtubed -c config.yml
```

## 备注：如何配置 Filebeat 写入 Logtubed

**必须使用 6.x 版本的 Filebeat**

```
# filebeat.yml

filebeat.inputs:
  - type: log
    # 日志文件路径，filebeat 比较傻，似乎不支持 **/* 格式
    paths:
      - "/var/log/logtube-logs/*.log"
      - "/var/log/logtube-logs/*/*.log"
      - "/var/log/logtube-logs/*/*/*.log"
      - "/var/log/logtube-logs/*/*/*/*.log"
      - "/var/log/logtube-logs/*/*/*/*/*.log"
    # 多行分割模式
    multiline.pattern: '^\[\d{4}[/-]\d{2}[/-]\d{2} \d{2}:\d{2}:\d{2}.\d+'
    multiline.negate: true
    multiline.match: after
output.redis:
  # Redis 输出地址
  # Filebeat 会随机写入一台主机，因此 Logtubed 可以部署多台，互不影响
  hosts: ["10.0.0.1", "10.0.0.2", "10.0.0.1"]
  datatype: "list"
  key: "xlog"
```
