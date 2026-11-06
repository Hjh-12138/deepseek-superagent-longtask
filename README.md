# DSH Longtask

DSH 通用长任务插件：在用户确认目标、完成标准、专家分工和预算后，持续执行、等待协作、恢复任务并完成独立验收。

当前核心版本：`0.1.2-rc.4`，仍为发布候选版。宿主兼容验证覆盖 DSH SDK `0.2.1-rc.5` 和 `0.2.2-rc.1`。金额账本按请求用量和价格表估算费用；它不是平台实扣账单。

## 目录

| 目录 | 用途 |
| --- | --- |
| [`dsh-longtask`](./dsh-longtask/) | 正式插件、单元/集成测试、配置示例和打包校验工具 |
| [`dsh-longtask-e2e`](./dsh-longtask-e2e/) | 开发联调驱动、离线夹具与实机验收判定器 |

正式安装只需要核心插件。测试引用同级联调驱动，因此开发时请保留仓库目录结构。运行数据、真实会话、机器配置、已安装 SDK 副本及历史实机报告保留在本地。

## 开发与离线测试

建议使用 Node.js 24 和 npm。

```sh
git clone https://github.com/Hjh-12138/dsh-longtask.git
cd dsh-longtask/dsh-longtask
npm ci --ignore-scripts
npm test
```

开发依赖固定在 SDK `0.2.1-rc.5`，正式插件的 peer 范围同时支持已验证的两个版本。离线测试不调用真实模型，也不需要账号凭证；测试会在系统临时目录创建独立数据和专家夹具。真实专家库扫描是可选检查，可通过 `DSH_LONGTASK_TEST_EXPERTS_ROOT` 指定已有专家目录；未设置时仅跳过该项。

## 使用与打包

在 DSH 普通对话中提出任务，确认工具会要求用户填写人民币总上限；预算建议不会自动成为用户授权。专家会话共享同一任务预算，独立验收通过后交付。

- [正式入口与功能](./dsh-longtask/README.md)
- [安装、升级与验证边界](./dsh-longtask/RELEASE.md)
- [实例配置示例](./dsh-longtask/release/config.example.yml)
- [更新记录](./dsh-longtask/CHANGELOG.md)

从核心目录构建并生成安装包：

```sh
npm run build
npm pack
```

`npm pack` 的文件白名单仅包含正式插件所需文件，不包含联调驱动、测试、运行数据或依赖副本。`release/verify-installed.mjs` 是早期 rc.1 首次安装的历史校验器，含特定版本及空库假设，不适用于已有任务的升级。

## 当前边界

单次输出截断可在原预算内有限续做；活动命令和专家回复使用持久等待机制。用量缺失、费用无法核对或预算不足会暂停，升级不会清除这些记录。rc.4 已补充并行 Worker 截断回归，仍需新的收费实机任务验证完整交付。内置价格有有效期，使用前应核对模型价格配置。
