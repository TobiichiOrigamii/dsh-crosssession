# dsh-session-hub

给 [DeepSeek Harness (DSH)](https://github.com/deepseek-ai/deepseek-harness) 的**跨会话协作插件**：持久会话团队、任务派发与排序队列、审批门、会话级任务板，以及输入框 `@` 提及。

> 让多个持久会话像团队成员一样互相派活、汇报进度，人和 agent 都能在右侧栏看到任务状态。

- 包名：`dsh-session-hub` · 版本：`0.1.0` · License：MIT
- 运行位置：DSH Host 插件（`index.js`）+ Web Client 插件（`client.js`）+ 独立看板页面（`board.html`）

## 功能

- **跨会话 / 跨工作区消息**：`session_send` 把消息或任务投递给任意会话；目标空闲会自动唤醒，运行中则排队到下一轮
- **持久会话团队**：`session_create` 在指定工作区创建持久会话，支持别名、花名册与团队状态查询
- **任务板**：每个任务有 `queued / running / blocked / done / failed / cancelled / awaiting-approval` 状态；完成结果自动结算
- **串行队列**：同一成员同时只跑一个任务，其余排队，当前任务结束后自动派发下一个
- **审批门**：命中 `policy.json` 的敏感工作区，任务/消息先挂起为「待批准」，由用户批准后才投递
- **会话级看板**：在 DSH 原生右侧栏打开「任务板」标签，只显示**当前会话参与过**的任务、消息与对端成员
- **`@` 提及成员**：输入框里 `@` 可呼出成员候选，选中插入可编辑的 tag 样式提到的会话
- **安静模式**：报告/通知压成单行，可改为只写看板（`deliverReports: board`），用 `session_inbox` 后台读取
- **持久化**：别名、成员、任务、消息、投递台帐全部落盘，DSH 重启后恢复

## 环境要求

- DeepSeek Harness `>= 0.2.0`（DSH Desktop 或 Web profile 均可）
- 看板与 `@` 提及需要 Web Client 环境（Desktop 默认满足）

## 安装

### 从插件市场（发布后）

```bash
dsh plugin install dsh-session-hub
```

或在 DSH 的插件市场中搜索 `dsh-session-hub` 安装，然后在配置里启用。

### 本地开发加载（file://）

在 `profiles/web/cordis.patch.yml` 里加入：

```yaml
- insert:
    - id: session-hub
      name: 'file:///absolute/path/to/dsh-session-hub/index.js'
      config:
        quietMode: true
        approvalTimeoutSeconds: 300
```

保存后 DSH 会热加载；修改 `index.js` 后需改 `id` 与 `?build=N` 触发重新加载（或重启 DSH）。

## 快速开始

```
# 创建持久会话并派任务（目标空闲会自动唤醒）
session_create({ workspace: "D:\\Code\\my-project", alias: "backend", title: "后端任务机" })
session_send({ target: "backend", message: "实现登录接口并跑通测试", kind: "task", taskTitle: "登录接口" })

# worker 完成后（无需传 id，插件按投递台帐自动对应任务）
session_report({ status: "done", summary: "登录接口已实现，测试通过" })

# 查看团队与任务板
session_team_status()
session_hub_board()
```

## 输入框 `@` 提及

输入 `@` 会呼出成员候选；选中后插入 `@别名`，并渲染为**可编辑的 tag 样式**（本质仍是文本，可继续编辑/删除；未匹配的 `@` 保持普通字符）。

> 触发边界遵循 DSH 输入语法：`@` 需要位于行首、空格或标点之后。`让 @backend 发送1` ✅；`让@backend发送1`（中文紧贴）不会触发候选。

## 会话级看板

- 右侧栏标签：`任务板`（类型 `sessionHubBoard`）
- 只显示当前会话参与的任务/消息与相关成员，2 秒刷新、只读
- 打开方式：会话页头「任务板」按钮；或每个浏览器会话首次加载自动打开一次
- 样式源文件：`board.html`（浅/深色、可折叠区块、tag 筛选、消息正文展示）

## 工具（18 个）

| 工具 | 说明 |
|---|---|
| `session_list` | 列出工作区/会话（只读，不唤醒） |
| `session_create` | 创建持久会话（自动注册工作区，继承调用方 preset/模型） |
| `session_send` | 发消息/派任务；空闲唤醒、运行中排队、`urgent` 插步 |
| `session_reply` | 回复收到的跨会话消息（`messageId` 可省略） |
| `session_report` | 上报任务结果 done/blocked/failed（`taskId` 可省略） |
| `session_task_status` | 查看任务（单个或全部） |
| `session_team_status` | 成员花名册 + 任务板概览 |
| `session_alias` | 绑定/清除会话别名 |
| `session_bridge_log` | 最近跨会话消息时间线 |
| `session_hub_board` | 返回看板地址与统计 |
| `session_broadcast` | 群发消息/任务（同样受审批策略约束） |
| `session_inbox` | 后台读取发给本会话的跨会话消息（不写入对话） |
| `session_export_log` | 导出任务板/时间线 Markdown（限 `harness\session-hub\`） |
| `session_purge_log` | 清理消息账本/已结束任务 |
| `session_task_approve` | 批准/拒绝「待批准」任务（仅调度方） |
| `session_task_cancel` | 取消任务（调度方或执行方） |
| `session_delete` | 删除会话（可选注销空工作区，文件不动） |
| `session_workspace_delete` | 注销工作区登记（文件不动） |

## 配置

```yaml
- id: session-hub
  config:
    completionNotify: true      # 任务结束时发送完成通知
    deliverReports: chat        # chat=进对话；board=只写任务板/收件箱
    quietMode: true             # 紧凑格式（会话名：正文 / 单行报告）
    noticeTextLimit: 160        # 通知/报告截断长度
    maxHops: 8                  # 回复链上限
    maxMessagesPerMinute: 20    # 每目标限流
    approvalWorkspaces: []      # 需审批的 workspace 路径模式
    approvalTimeoutSeconds: 300
```

### 审批策略 `policy.json`

数据目录 `harness\session-hub\policy.json`（热读取，改完立即生效）：

```json
{
  "approvalWorkspaces": ["production", "正式", "/prod/i"],
  "approvalTimeoutSeconds": 300
}
```

- 匹配目标会话 `cwd`（不区分大小写包含；`/.../` 为正则）
- 空数组表示不启用审批；超时任务自动置为 `cancelled`

## 消息格式与后台投递

对话里只显示 `会话名：正文`，不显示任何 id。后台 `state.deliveries` 记录每个接收方最近的 `messageId / taskId / kind / from`，因此：

- `session_report({status, summary})` 自动对应「最近派给该会话的任务」
- `session_reply({message})` 自动对应「最近发给该会话、未回复的消息」
- 需要精确定位时仍可显式传 `taskId` / `messageId`

## 文件结构

```
index.js            Host 插件：工具、队列、审批、持久化、投递、看板服务
client.js           Client 插件：右侧栏「任务板」标签 + @ 提及来源
board.html          看板页面（独立样式文件，随 Host 路由提供）
policy.example.json 审批策略示例
LICENSE             MIT
```

运行时数据位于 `$DSH_HOME/session-hub/`（`state.json` / `policy.json`），不随包发布。

## 安全

- 看板服务仅监听 loopback，响应带 CSP（`default-src 'none'`，`frame-ancestors` 限定 DSH origin）、`nosniff`、`no-referrer`
- `session_export_log` 仅允许写入 `harness\session-hub\`
- `state.json` 损坏时先备份为 `state.json.corrupt-*`
- 跨会话消息本质是发给另一个 agent 的 prompt，安全性依赖审批策略、hop 上限与接收方自身沙箱

## 开发

```bash
node --check index.js
node --check client.js
```

修改 Host 插件后在 patch 中改 `id` 和 `?build=N` 以绕过 ESM 缓存；`client.js` 由 DSH client-modules 自动扫描。

## License

MIT
