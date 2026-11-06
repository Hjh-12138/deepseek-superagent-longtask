# DSH 通用长任务插件

核心插件可独立提供长任务操作入口，不需要安装`dsh-longtask-e2e`。用户在普通DSH对话中提出要求，当前Agent通过以下正式工具操作同一任务内核。

## 用户怎样操作

例如：

> 把比较两种存储方案作为长任务，交付比较表和推荐结论。先给我方案和专家分工，开工前让我填写人民币预算。

DSH检索专家、提出目标/完成标准/产物路径/分工/独立验收人，可建议预算金额。用户可调整待确认方案。确认工具通过宿主userQuestions展示完整方案并要求用户填写人民币总上限（例如5或2.50）；提交金额即确认开工。建议金额不自动生效，未填写或关闭面板均不开工。

运行中可直接说：

- “列出我的长任务。”
- “查看任务 task_… 的状态、成果和用量。”
- “暂停任务 task_…。”
- “恢复任务 task_…，沿用原预算。”
- “取消任务 task_…。”

恢复也需宿主确认；预算不足或答复投递结果未知时继续保持暂停。不会自动加额或把unknown当成未投递重发。模糊目标应先在对话中澄清；本插件不另开一个Router模型。

也可回答任务澄清、补充信息、确认纠错重开或更换执行专家；新增要求用关联待确认任务单独计预算。完整安装、升级、回退和发布验收步骤见[RELEASE.md](RELEASE.md)。当前是`0.1.2-rc.5`候选版，已补齐工具审批等待与Router通知的回归，尚待真实桌面复验。

Worker 等待工具审批时，主会话会收到待办摘要，包括 Worker 会话编号、工具、原因和审批编号；任务查询也会列出全部待审批项。请打开对应 Worker 的 DSH 审批卡片批准或拒绝，Router 中发送“继续”不构成授权。决定后由宿主继续原工具调用，插件只更新等待与通知，不额外催跑。取消、撤权或轮次结束会使等待失效；运行时重载后原审批不能重放，相关任务暂停供用户核对。

## 正式工具

| 工具 | 用途 |
| --- | --- |
| longtask_experts | 检索本地专家简介和稳定相对路径 |
| longtask_propose | 提出方案，或调整从未执行的待确认方案；不派工 |
| longtask_confirm | 展示指定版本的方案并请求宿主用户确认；同意后开工 |
| longtask_control | 暂停/恢复/取消、答复澄清、补充/澄清、纠错重开、替换执行专家；不自动加额 |
| longtask_tasks | 分页列表，或指定taskId查看方案、状态、预算、产物引用和验收进度 |

`longtask_propose`必须明确`goal`、`criteria[{id,text}]`、绝对路径`deliverables`、`executorRelPaths`、`acceptorRelPath`；可传`suggestedBudgetCny`（十进制字符串，仅建议），不再接受`budgetTokens`或模型代填的金额上限。多专家方案还需`assignments`。验收人必须独立。修改待确认方案时带`taskId`和`expectedVersion`；确认时带当前`taskId`和`version`。

新增要求提供`parentTaskId`，生成关联proposed任务。`longtask_control`的`answer/supplement/clarify`提供`text`；`reopen`提供`criteria`和`expectedVersion`；`replace_executor`提供`addRelPath/removeRelPath/expectedVersion`，可附`pendingItems`。答复恢复、纠错重开与换人均先走宿主确认，拒绝时不变更任务；未知用量和未知投递不能通过这些入口绕过。

执行者、验收者及其后代不能调用这些管理工具。真实宿主创建Worker时会从其工具面隐藏这些入口；正文和单调guard同时校验任务身份。任务内专家通信仍使用原有协议。

## 宿主与配置

需要DSH的agents、sessions、tools服务，新任务开工所需的userQuestions.ask输入通道，以及恢复等动作所需的approval.request确认通道。缺通道时保持原状态，不降级成自动开工。SDK参照0.2.1-rc.5，并已使用桌面实际携带的0.2.2-rc.1完成同套回归。

安装核心bundle后，在当前profile配置：

- `dataDir`：正式任务状态与日志目录。
- `agencyAgentsRoot`：本地专家定义库，含divisions.json。
- `workerCwd`：存在的绝对任务工作目录，正式使用时设置到目标工作区。
- `workerProvider`、`workerModel`、`workerMaxTokens`、`workerPreset`：宿主可用模型和能力集。
- `acceptorReadOnly`：正式bundle显式启用，验收只允许读取/检索工具，写入和shell被拒绝；不依赖另装preset。旧profile默认兼容旧行为，可显式启用。
- `acceptorPreset`：可选验收能力集；启用只读白名单后仍受其约束。
- `modelPrices`：按精确provider/model配置人民币价格；数组覆盖内置表。金额与单价均为十进制字符串，单位为元/百万Token，详见配置示例。缺价、过期或币种不支持时拒绝金额请求。

任务入口会显示当前工作目录；交付路径必须为绝对路径。工具权限与沙箱仍由DSH宿主管理，不会因插件生成方案而扩大权限。

提案前普通DSH对话的模型费用由该宿主会话显示；正式工具本身不发模型请求。已确认任务的账本覆盖托管执行、验收、返工及监督等请求；本入口不接管用户主会话的全部消费。

## 金额预算的口径

用户只需设人民币上限。请求前按输入估算和最大输出预留金额，所有专家共用同一事务账本；响应后依据SDK返回的互斥用量分项和本次请求价格快照核算。失败重试单独计费，重复事件不重复记账；费用未知保留预留并暂停。金额内部以纳元整数持久化，不逐次舍入到分。

内置价格仅支持`deepseek-official/deepseek-flash`，来源为[DeepSeek官方价格页](https://api-docs.deepseek.com/zh-cn/quick_start/pricing/)。2026-11-06核对的高峰价为：缓存输入0.04、未缓存输入2、输出8元/百万Token；缓存写入按未缓存输入保守核算。为避免峰谷边界和节假日误判，首版统一按高峰价核算，价格有效期至2026-12-06北京时间0时。**核算费用不是平台实扣**，空闲时段实际费用可能更低；价格过期需要更新配置，不静默沿用。

预算包含本任务托管模型请求，不包含用户主会话或第三方付费工具；账户余额差不用于给任务归属费用。保留收尾、验收和其他专家报告额度，并按金额压力提前收尾。单次输出截断有已结算用量且原预算允许时接续原Worker；连续截断最多自动接续两次，仍无进展则暂停。

历史Token任务保持原账本和原上限，不自动折算人民币；切换预算单位需新建任务。首轮输入仍依赖声明基线，宿主隐藏的动态系统提示和工具定义不完全可见，因此本插件不是供应商侧的绝对消费硬限额。

## 构建与分发

```powershell
npm run build
npm test
npm pack
```

包白名单包含`dist/src`、bundle、README、RELEASE、CHANGELOG、配置示例和只读诊断器（npm自动附package.json），不打包测试代码、任务数据库、验收证据或E2E消费者。开发者可保留E2E源码作回归，但正式profile不必挂载它。

`getRuntimeInfo()`返回`userEntryProtocol:3`、`moneyAccountingProtocol:1`、`routerNoticeProtocol:1`和`userEntryReady`，便于核对是否生效。本次包含金额请求及主会话状态回传协议，首次升级请完整退出并重启DSH，避免子模块缓存沿用旧代码；检测到旧模块会阻止就绪。加载插件不会自行新建或确认任务。

每次模型重试经`agent/request`重新准入；失败attempt从SDK stream提取用量。缺用量不等于0，会保留未结算预留并持久阻止继续执行。新协议标记为`userActionsProtocol:1`和`requestAccountingProtocol:1`。默认数据为当前用户的`~/.dsh/longtask`，不自动导入开发联调任务。

## 主会话状态反馈

任务由正式入口的实际调用方会话绑定Router，不由模型填写会话ID。后台发生自动暂停、单次输出截断接续、独立验收交付时，只把任务ID、状态、原因及预算摘要写到这个主会话。使用DSH的notice来源元数据，折叠行也有状态摘要；展开可查看原因和操作提示。不转发专家定义、原始讨论或产物全文。

这些记录不会调用followup/steer，也不为了播报再请求模型；Router下一次正常交互可读取。主会话未加载时持久保留待投递，不借其他会话广播；恢复加载后核对稳定消息ID，避免重复显示。过期暂停状态不在任务恢复后误投。

宿主Worker中的“发送继续”是通用截断提示，不能解除插件任务暂停。单次输出截断满足条件时插件会自动接续；已暂停时应在主会话说“查看任务 task_… 的暂停原因”，处理后再说“恢复任务 task_…，沿用原预算”。金额不足或费用待核对不能靠继续绕过。旧任务没有Router绑定时不猜测收件人，用户从主会话批准恢复等操作后绑定该主会话。
