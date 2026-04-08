# CLAUDE.md - 项目上下文与开发规范

## 项目概述

IVD（体外诊断）项目管理平台，基于 R Shiny + PostgreSQL + Docker + Nginx 架构。

### 核心组件
- **app.R** — 主业务应用（甘特图项目管理 + 会议决策），单文件 Shiny 应用，~6600 行
- **db_maintain.R** — 数据库维护工具
- **PostgreSQL 15** — 数据库，schema 名 `public`，业务表以中文编号命名（01~10）
- **Nginx** — 反向代理网关，4个 app-shiny 容器做负载均衡，2个 maintain 容器，2个 auth 容器

### 关键数据库表
- `01医院信息表` — 医院
- `02仪器资源表` — 仪器（FK → 01, 05）
- `03医院_项目表` — 医院-项目关联（FK → 01, 04）
- `04项目总表` — 项目总表（FK → 05人员表）
- `05人员表` — 人员（含工号、哈希密码、数据库权限等级、人员状态）
- `06~10` — 样本资源、审计日志、阶段定义、阶段实例、会议决策

### 权限系统
`current_user_auth()` reactive 返回：
- `allow_all` — TRUE 时无需项目级过滤（super_admin, manager）
- `allow_none` — TRUE 时完全拒绝
- `allowed_subquery` — SQL 子查询字符串，用于 WHERE 过滤可访问项目
- `work_id`, `name` — 当前用户工号和姓名
- `is_super_admin`, `can_manage_project` — 权限标记

Nginx 通过 HTTP header `X-USER` 传递已认证用户身份。

### 部署架构
- **WSL** 构建 Docker 镜像 → `docker save` → `cp` 到 `/mnt/c/IVD_Project/` → Windows 端 `docker.exe load` → `docker.exe compose up -d`
- 所有镜像 tag 统一使用 `YYYY_mm_dd_HH_MM` 时间戳格式（如 `2026_04_03_10_30`），每次部署自动生成，不重用旧 tag
- `Dockerfile.update` 基于已有镜像增量更新（`FROM ivd_shiny:20260305`），只 COPY app.R
- 4 个 `app-shiny` 容器共享同一镜像，Nginx 轮询负载均衡

## 开发规范

### 调试时必须检查所有容器
项目使用 4 个 app-shiny 容器做负载均衡，**调试时必须检查全部 4 个容器的日志**：
```bash
docker.exe logs ivd_project-app-shiny-1-1 --tail 20
docker.exe logs ivd_project-app-shiny-2-1 --tail 20
docker.exe logs ivd_project-app-shiny-3-1 --tail 20
docker.exe logs ivd_project-app-shiny-4-1 --tail 20
```
不能只看一个容器，因为 Nginx 轮询分发请求，错误可能出现在任何一个容器中。

### 部署流程

所有镜像（ivd_shiny、ivd_maintain_shiny 等）统一使用 `YYYY_mm_dd_HH_MM` 时间戳作为 tag，每次部署自动生成：

**app-shiny 部署：**
1. `TAG=$(date +%Y_%m_%d_%H_%M)`
2. `docker build --no-cache -f Dockerfile.update -t ivd_shiny:$TAG .`
3. `docker save ivd_shiny:$TAG -o /tmp/ivd_shiny_$TAG.tar && cp /tmp/ivd_shiny_$TAG.tar /mnt/c/IVD_Project/`
4. 更新 `/mnt/c/IVD_Project/docker-compose.yml` 中 4 个 app-shiny 容器的 `image: ivd_shiny:$TAG`
5. `docker.exe load -i C:\\IVD_Project\\ivd_shiny_$TAG.tar`
6. `docker.exe compose -f C:\\IVD_Project\\docker-compose.yml up -d --force-recreate app-shiny-1 app-shiny-2 app-shiny-3 app-shiny-4`
7. 等 8 秒后检查全部 4 个容器日志

**maintain-shiny 部署：**
1. `TAG=$(date +%Y_%m_%d_%H_%M)`
2. `docker build --no-cache -f Dockerfile.maintain -t ivd_maintain_shiny:$TAG .`
3. `docker save ivd_maintain_shiny:$TAG -o /tmp/ivd_maintain_shiny_$TAG.tar && cp /tmp/ivd_maintain_shiny_$TAG.tar /mnt/c/IVD_Project/`
4. 更新 `/mnt/c/IVD_Project/docker-compose.yml` 中 2 个 maintain-shiny 容器的 `image: ivd_maintain_shiny:$TAG`
5. `docker.exe load -i C:\\IVD_Project\\ivd_maintain_shiny_$TAG.tar`
6. `docker.exe compose -f C:\\IVD_Project\\docker-compose.yml up -d --force-recreate maintain-shiny-1 maintain-shiny-2`
7. 等 8 秒后检查 2 个 maintain 容器日志

### Shiny 性能：禁止批量创建 observeEvent / output 闭包

**绝对禁止**在 `for` 循环中注册 `observeEvent`、`observe`、`output$x` 闭包。Shiny 在 `server()` 函数体执行时逐一创建闭包并注册到 session，每个闭包消耗 ~5-10ms。循环 3000 次的 `observeEvent` 仅注册就吃掉 25-35 秒，导致首屏白屏。

**历史教训**：本项目的会议决策模块曾为每条可能的决策行（for 1:3000）和每个执行人按钮（for 1:5000）预注册闭包，server() 注册阶段 35 秒，首屏 53 秒。优化后注册阶段降至 ~2 秒，首屏 ~5 秒。

#### 替代方案

| 场景 | 禁止写法 | 正确写法 |
|------|---------|---------|
| N 个动态按钮的点击处理 | `for(i in 1:N) observeEvent(input[[paste0("btn_",i)]], ...)` | **JS 事件委托**：所有按钮统一 class/id 前缀，前端 jQuery `$(document).on('click', selector, fn)` 用 `Shiny.onInputChange('统一input名', id_nonce)` 发送，R 端只需 **1 个** `observeEvent(input$统一input名)` |
| N 个动态输出 | `for(i in 1:N) output[[paste0("ui_",i)]] <- renderUI(...)` | **懒注册**：先注册 1 个容器 `output$dynamic_area <- renderUI(...)`，在用户触发操作时才生成具体内容；或用 `insertUI` / `renderUI` + lapply 按需生成 |
| actionLink/actionButton 的重复点击 | 同一 value 的 `Shiny.setInputValue` 不触发 observeEvent | 值必须每次不同：拼接 `Date.now()`（JS）或 `Sys.time()` nonce（R）；`observeEvent` 监听的 reactiveVal 也需加 nonce 保证值变化 |

#### JS 事件委托模板

```javascript
// 在 tags$head(tags$script(HTML("..."))) 中注册一次
$(document).on('click', '.your-btn-class', function(e) {
  e.preventDefault();
  var id = $(this).attr('data-id') || this.id.replace('prefix_', '');
  Shiny.onInputChange('your_unified_input', id + '_' + Date.now());
});
```
```r
# R 端只需一个 observeEvent
observeEvent(input$your_unified_input, {
  raw <- input$your_unified_input
  id <- as.integer(sub("_.*$", "", as.character(raw)))
  if (is.na(id)) return
  # 处理逻辑...
}, ignoreInit = TRUE)
```

#### observeEvent 重复触发陷阱

`observeEvent` 仅在**值发生变化**时触发。两个常见陷阱：
1. `Shiny.onInputChange / setInputValue` 发送相同值 → 不触发。**必须拼接 nonce**（如 `id + '_' + Date.now()`）
2. 中间 reactiveVal 设置相同 list → 下游 `observeEvent(rv())` 不触发。**在 list 中加 `nonce = as.character(Sys.time())`** 保证每次值不同

### 代码注意事项
- `current_user_auth()` 没有 `$ok` 字段，用 `allow_none` 和 `allow_all` 判断权限
- PostgreSQL `json` 列通过 RPostgres 读取后是 `list` 类型，必须 `::text` 转换或 `as.character()` 后再处理
- 会议决策列表权限：无 `12` 关联的 `10` 行视为共性（全员可见）；有 `12` 时按 `11`.`04项目总表_id` 与 `allowed_subquery` 过滤（其他业务里对 `10` 的 `OR "04项目总表_id" IS NULL` 已不再适用，`10` 已去掉该列）
- `insert_audit_log()` 用于操作审计，参数：连接、工号、姓名、操作类型、目标表、目标行id、描述...
- `parse(file='app.R')` 验证语法，不等于运行正确
- 中文列名在 SQL 中必须用双引号包裹：`"项目名称"`

### 会议决策功能（10 + 12 + 11）
- 项目关联只通过 `12`→`11`→`04`；`10` 无 `04项目总表_id`。无 `12` 行 = 共性决策。
- 新建/编辑：选具体项目时必须至少关联一条 `11` 反馈且项目一致；选「共性决策」可不关联。
- 新建会议用弹窗维护「项目要点」（列表展示同项目汇总「问题与反馈」）；弹窗内可新增**项目级** `11` 行（`09项目阶段实例表_id` 为 NULL，仅 `04项目总表_id`），需已执行 `migrations/002_11_nullable_09_project_scope.sql`。
- 执行人从 `05人员表` 全体在职人员中选择
- 执行状态存储在 `决策执行人及执行确认` json 列：`{"姓名-工号": {"状态": "未执行/已执行", "说明": "..."}}`
- 当前用户可点击自己的执行人标签修改状态

### 甘特图滚动位置保持机制（切勿破坏）

**背景**：vis-timeline 的 `itemsData.clear() + add()` 会重建 DOM，导致纵向滚动位置重置。用户在甘特图内编辑保存后，期望滚动位置不变。

**两种更新模式**：
| 触发来源 | 模式 | 行为 |
|---------|------|------|
| 筛选器变化（类型/名称/负责人/参与人/医院等） | `full` | `itemsData.clear() + add()` 完全重建，正确移除被过滤掉的组和条目 |
| 用户交互保存（编辑阶段、保存进度、保存贡献者等 12 处 `btn_save_*`） | `incremental` | `itemsData.update()` 增量更新，保留滚动位置 |

**实现手段（JS 猴子补丁）**：
1. 前端在 `tags$script` 中对 timevis widget 的 `setItems`/`setGroups` 做 monkey-patch
2. JS 全局变量 `_updateMode` 控制 `'full'` 或 `'incremental'`
3. R 端 `gantt_use_incremental` reactiveVal 标记当前更新来源
4. `gantt_sql_filter_bundle` observe 中读取该标记，通过 `session$sendCustomMessage("ganttSetUpdateMode", mode)` 切换 JS 端模式
5. 随后调用 `setItems()`/`setGroups()`（timevis 原生 proxy），被猴子补丁拦截并根据模式选择更新方式

**关键约束（勿改）**：
- **必须**用 `setItems`/`setGroups`（timevis 原生 proxy），不能用自定义 `sendCustomMessage` 替代——绕过 timevis 内部消息路由会破坏筛选器耦合和会议页面选项
- `gantt_filter_state` **不能**包含 `gantt_force_refresh()`——否则 debounce 400ms 后会触发第二次 full 模式更新，覆盖 incremental 模式的滚动保持效果
- 所有用户交互保存点（12 处 `gantt_use_incremental(TRUE)` + `gantt_force_refresh(+1)`）必须在同一行成对出现
- 猴子补丁中 ID 比较必须用 `String()` 统一类型，否则 `indexOf` 严格等于判断失败导致组无法正确移除
