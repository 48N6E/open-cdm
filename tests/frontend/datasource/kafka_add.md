# Kafka 数据源添加

## Purpose

验证 Kafka 作为独立数据源类型出现在类型选择器中，新增页默认端口 9092、驱动家族为 Kafka Clients，并覆盖 2.8 到 4.0 的客户端版本选择。

## Scope

- 页面与路由：实例列表 `/#/datasource`、新增数据源 `/#/datasource/add?dsType=Kafka`。
- 入口：实例列表“新增”按钮和数据源类型选择弹窗。
- 关联接口与状态：全局设置 `dsSupportNames`、`dsSettingDef.Kafka`。
- 关联源码：`frontend/src/components/function/CustomIcon.vue`、`backend/clouddm-plugins/clouddm-ds/ds-kafka/`。
- 不覆盖：真实 Kafka 集群连接测试、保存实例、消费消息结果内容和权限工单。

## Preconditions

- 本地 Alone 可通过 `http://localhost:8222` 访问，或前端 `npm run serve:dm` 代理到该后端。
- 使用已登录且拥有实例查看、新增权限的测试账号。
- 后端已加载 `ds-kafka` 插件。
- 本流程默认不提交新增数据源。若执行了连接测试或保存，必须在 Cleanup 中删除。

## Test Data

| 编号 | 数据说明 | 构造方式 | 唯一标识 | 清理方式 |
|---|---|---|---|---|
| KAFKA01 | Kafka 类型选择 | 类型选择弹窗中选择 Kafka | 不适用 | 无需清理 |
| KAFKA02 | 默认连接字段 | 进入新增页后查看端口、驱动家族、SASL | 不适用 | 关闭新增页即可 |

## Suites

### KAFKA-SMOKE-01 类型选择器可见

- 风险/目的：P0，确认 Kafka 出现在添加数据源类型列表并带有图标。
- 初始路由与状态：已登录，位于 `/#/datasource`。
- 测试数据：KAFKA01。
- Chrome 操作：点击“新增”，在类型选择弹窗中找到 Kafka。
- 预期结果：类型名显示为 Kafka；图标可见且不是空白占位。
- 恢复/清理：关闭类型选择弹窗。

### KAFKA-MAIN-01 新增页默认值

- 风险/目的：P0，确认默认端口、Kafka Clients 驱动家族和 1.x–4.0 兼容客户端版本。
- 初始路由与状态：从类型选择确认后进入 `/#/datasource/add?dsType=Kafka`。
- 测试数据：KAFKA02。
- Chrome 操作：查看端口、驱动家族、驱动版本列表、安全类型和 SASL 机制。
- 预期结果：端口为 `9092`；驱动家族为 Kafka Clients；默认驱动版本为 `3.9.1`；版本列表包含 `2.8.2`、`3.3.2`、`3.6.2`、`3.9.1`、`4.0.0`；安全类型可选无认证 / 用户名密码；SASL 机制可选 PLAIN / SCRAM-SHA-256 / SCRAM-SHA-512。
- 恢复/清理：离开新增页，不保存。

## Cleanup

1. 不保存未完成的新增数据源。
2. 若误保存，删除该 Kafka 实例。

## Skip Conditions

- 后端未加载 `ds-kafka` 插件时跳过，并记录覆盖缺口。
