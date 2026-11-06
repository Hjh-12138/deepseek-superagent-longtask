# Longtask 开发联调驱动

本目录用于离线夹具、事件判定和真实 DSH 宿主联调。核心插件可独立安装，正式 profile 无需启用此驱动。

`index.js` 只消费显式联调请求，默认 `autoRun:false`。默认记录目录由源码相对路径解析到同级核心目录下的 `.longtask/e2e`，也可通过实例配置覆盖。

| 文件 | 用途 |
| --- | --- |
| `request-run.mjs` | 阶段一、二场景请求（含 pair、wrapup、gap） |
| `request-stage3.mjs`、`stage3-*.mjs` | 暂停、恢复、用户答复、后台等待等生命周期验证 |
| `request-stage4.mjs`、`stage4-*.mjs` | 实机验收与故障注入 |
| `evidence.mjs`、`*-fixture.mjs` | 证据判定和可重复测试夹具 |

核心 `npm test` 使用其中的离线接口，模拟模型响应和宿主事件。实际启用驱动并提交运行请求会创建真实任务、消耗已确认预算；部分故障注入场景会要求退出或重启 DSH，应在开发 profile 中按场景运行。

本目录保留先前按 Token 上限编排的开发验收协议；正式用户任务的金额预算入口见同级核心插件文档。不要用旧联调请求代替正式用户确认。
