# DSH Longtask

DSH 通用长任务插件：在用户确认目标、完成标准、专家分工和预算后，持续执行、等待协作、恢复任务并完成独立验收。

当前核心版本：`0.1.2-rc.19`，仍为发布候选版。宿主兼容验证覆盖 DSH SDK `0.2.1-rc.5` 和 `0.2.2-rc.1`。金额账本按请求用量和价格表估算费用；它不是平台实扣账单。

rc.19包含实测行为识别与业务模块依赖调度修复，见[更新记录](./dsh-longtask/CHANGELOG.md)和[发布说明](./dsh-longtask/RELEASE.md)。归档rc.18包保持不变，安装核对与完整桌面任务验收分别记录。

## 目录

| 目录 | 用途 |
| --- | --- |
| [`dsh-longtask`](./dsh-longtask/) | 正式插件、单元/集成测试、配置示例和打包校验工具 |

仓库仅维护核心插件及其单元/集成测试，构建和测试不依赖另一个联调插件。历史开发联调套件已移出仓库，运行数据、真实会话、机器配置、已安装SDK副本及历史实机报告保留在本地。

专家库通过 `agencyAgentsRoot` 配置，定义不内置在核心包中。目录递归扫描 `divisions.json` 声明的分类，支持中文/英文关键词检索。`longtask_experts` 可用 `task` 提交完整目标、技术栈与交付要求，获得职责和实际候选；`longtask_propose` 也会返回职责候选与兼任提示。推荐只读目录元数据，不另调模型或自动增加 Worker。

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

单次输出截断可在原预算内有限续做；活动命令、专家回复和工具审批使用持久等待机制。待审批列表及决定结果回传绑定的 Router，由用户在对应 Worker 的 DSH 审批卡片处理；通知本身不唤醒模型或授予权限。用量缺失、费用无法核对或预算不足会暂停，升级不会清除这些记录。rc.18 已有安装启动与历史验收附件导出核对，新增的同任务标准修订补测等完整桌面流程仍需复验；当前验证范围见[发布说明](./dsh-longtask/RELEASE.md#当前验证范围与待办)。内置价格有有效期，使用前应核对模型价格配置。
