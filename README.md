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
- 至少一次投递：publish / delivery / ack / createQueue / deadLetter / discard 均写 WAL 并 fsync，重启后按 WAL 重放；durable transition 先落盘再改内存，写失败时消息保持可重投状态
- 崩溃恢复：只截断正在写入那一段尾部未写完的记录；其余损坏、不可读一律 fail closed 拒绝启动
- 投递上限与死信队列：投递次数超过 `max-delivery-count` 的消息转入派生的死信队列 `<queue>.dlq`。`.dlq` 是保留后缀，整次转移由一条 WAL 记录表达，重启后消息仍在死信队列里；死信队列自身是终态，其中再次超限的消息持久化丢弃，不再派生 `<queue>.dlq.dlq`
- 消费端幂等：按 messageId 去重，业务 ack 过的消息重投时直接跳过
- 分层配置：环境变量 > YAML > 默认值
- 并发模型：gRPC 服务端的每个请求均运行在一个虚拟线程上（`Executors.newVirtualThreadPerTaskExecutor()`）
- gRPC 状态映射：
  - 队列不存在 → `NOT_FOUND`
  - 重复消息 → `ALREADY_EXISTS`
  - 参数非法 → `INVALID_ARGUMENT`

## TODO

- 死信队列增强：目标固定按 `<queue>.dlq` 派生，不支持自定义目标；死信消息暂不支持重投回源队列或按消息 TTL 过期清理
- WAL 滚动与压缩：目前只分段、不滚动，恢复时全量读入内存
- 过期消息的重投依赖下一次 consume 触发，暂未实现周期性重投的后台线程
- 消费端去重只在内存中，进程重启后失效
