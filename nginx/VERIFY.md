# Nginx 网关与登录鉴权自检

## 0. Windows 部署注意（CRLF）

若 `default.conf` 在 Windows 上编辑或保存，可能带 CRLF 换行，挂载进 Linux 容器后会导致 `location ^~ /login/` 等前缀无法匹配，从而 `/login/` 被错误转到甘特图。当前配置已改为正则 `location ~ ^/login/(.*)$` 以减轻该问题。若仍异常，请确保挂载的 `default.conf` 为 **LF 换行**（可用 VS Code / Notepad++ 转为 “Unix (LF)” 后保存）。

## 1. 确认容器内配置

```powershell
docker exec ivd_gateway cat /etc/nginx/conf.d/default.conf
```

应包含登录 location（如 `location ~ ^/login/` 或 `location ^~ /login/`）且为 `proxy_pass http://auth_shiny`。

## 2. 从网关内测试 auth_shiny 鉴权（无 Cookie 应 401）

在 **PowerShell** 里用下面任一条即可（避免引号被吞掉）：

```powershell
docker exec ivd_gateway curl -s -i -H "X-Auth-Verify: 1" http://auth_shiny/
```

看输出第一行：应为 `HTTP/1.1 401 Unauthorized`。若为 `200 OK`，说明 login.R 未识别鉴权头。

或只打一台 auth 容器（网关内）：

```powershell
docker exec ivd_gateway curl -s -i -H "X-Auth-Verify: 1" http://auth-shiny-1:3838/
```

## 3. 从网关内测试 /login/ 是否到 auth_shiny

```powershell
docker exec ivd_gateway curl -s -i http://127.0.0.1/login/
```

应返回登录页 HTML，且第一行 `HTTP/1.1 200 OK`；正文里应有「临床项目管理系统 - 登录与注册」。

## 4. 浏览器

- 无痕访问 `http://localhost/login/`（末尾要有 `/`）应直接看到登录页。
- 无痕访问 `http://localhost/` 应先 302 到 `/login/`，再显示登录页。
