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
- `Dockerfile.update` 基于已有镜像增量更新（`FROM ivd_shiny:20260305`），只 COPY app.R
- 4 个 `app-shiny` 容器共享同一镜像 `ivd_shiny:20260331`，Nginx 轮询负载均衡

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
1. `docker build -f Dockerfile.update -t ivd_shiny:20260331 .`
2. `docker save ivd_shiny:20260331 -o /tmp/ivd_shiny_20260331.tar`
3. `cp /tmp/ivd_shiny_20260331.tar /mnt/c/IVD_Project/`
4. `docker.exe load -i C:\\IVD_Project\\ivd_shiny_20260331.tar`
5. `docker.exe compose -f C:\\IVD_Project\\docker-compose.yml up -d app-shiny-1 app-shiny-2 app-shiny-3 app-shiny-4`
6. 等 8 秒后检查全部 4 个容器日志

### 代码注意事项
- `current_user_auth()` 没有 `$ok` 字段，用 `allow_none` 和 `allow_all` 判断权限
- PostgreSQL `json` 列通过 RPostgres 读取后是 `list` 类型，必须 `::text` 转换或 `as.character()` 后再处理
- 业务 SQL 中的权限过滤必须包含 `OR "04项目总表_id" IS NULL` 以兼容"共性决策"等不关联项目的记录
- `insert_audit_log()` 用于操作审计，参数：连接、工号、姓名、操作类型、目标表、目标行id、描述...
- `parse(file='app.R')` 验证语法，不等于运行正确
- 中文列名在 SQL 中必须用双引号包裹：`"项目名称"`

### 会议决策功能（10会议决策表）
- 支持关联项目（`04项目总表_id`）或"共性决策"（`04项目总表_id = NULL`）
- 执行人从 `05人员表` 全体在职人员中选择
- 执行状态存储在 `决策执行人及执行确认` json 列：`{"姓名-工号": {"状态": "未执行/已执行", "说明": "..."}}`
- 当前用户可点击自己的执行人标签修改状态
