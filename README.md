# SakiMQ

一个基于 gRPC 的轻量消息队列，至少需要 Java 21 运行时。

> 个人项目。

## 模块

这是一个多模块的 Maven 项目。

| 模块 | 内容 |
| --- | --- |
| `protocol` | protobuf 定义（gRPC 服务接口、WAL 记录）与生成代码 |
| `common` | 配置加载、异常体系 |
| `broker` | 服务端：内存队列、WAL 持久化、gRPC 服务 |
| `producer` / `consumer` | 客户端库 |

## 已实现特性

- 队列操作：createQueue / publish / consume / ack / getQueueStats
- 可见性超时：消息投递后对其他消费者不可见，超时未 ack 自动重投
- 至少一次投递：publish / delivery / ack / createQueue 均写 WAL 并 fsync，重启后按 WAL 重放；ack 在队列锁内先落盘再改内存，写失败时消息保持 inflight
- 崩溃恢复：只截断正在写入那一段尾部未写完的记录；其余损坏、不可读一律 fail closed 拒绝启动
- 投递上限：超过 `max-delivery-count` 的消息进入死信队列（计划中）
- 消费端幂等：按 messageId 去重，业务 ack 过的消息重投时直接跳过
- 分层配置：环境变量 > YAML > 默认值
- gRPC 状态映射：
  - 队列不存在 → `NOT_FOUND`
  - 重复消息 → `ALREADY_EXISTS`
  - 参数非法 → `INVALID_ARGUMENT`

## TODO

- 死信队列：超限消息目前直接丢弃，暂未实现死信队列
- WAL 滚动与压缩：目前只分段、不滚动，恢复时全量读入内存
- 过期消息的重投依赖下一次 consume 触发，暂未实现周期性重投的后台线程
- 消费端去重只在内存中，进程重启后失效
