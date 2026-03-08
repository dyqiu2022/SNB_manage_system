# Nginx + Shiny + Docker 部署踩坑记录

从零搭建「Nginx 网关 + 登录鉴权 (auth_shiny) + 甘特图业务 (app_shiny)」到最终跑通，一路遇到的问题与解决办法汇总，供日后部署或排错参考。

---

## 一、整体架构与预期流程

- **用户入口**：只访问根路径 `http://localhost/`（甘特图）。
- **鉴权**：Nginx 对每次访问 `/` 先发内部子请求到 `auth_shiny` 的鉴权接口；无有效 Cookie 则返回 401，Nginx 对浏览器 302 到 `/login/`。
- **登录页**：`/login/` 及 `/login/*` 全部由 `auth_shiny`（login.R）提供；登录成功后写 Cookie 并跳转回 `/`。
- **再次访问 /**：带 Cookie 的请求经鉴权 200 后，Nginx 把 `X-User`（工号）带给 app_shiny，甘特图按工号查权限并展示数据。

涉及组件：PostgreSQL、NocoDB、Metabase、4× app_shiny、2× auth_shiny、Nginx 网关，均在 Docker Compose 内。

---

## 二、Nginx 与网关

### 1. 登录/鉴权不要用「路径」区分，用自定义 Header

**现象**：最初设计是 auth_shiny 在「路径」`/verify` 上做鉴权、在 `/login` 提供登录页。结果 Nginx 把请求转发到 `auth_shiny/verify` 或 `auth_shiny/login` 时，Shiny 对「非根路径」直接返回 **404 Not Found**（Shiny 默认只服务根路径，不会按 PATH_INFO 路由）。

**解决**：鉴权不依赖路径，改为「自定义 Header」：

- Nginx 的 `location = /auth-verify` 内部子请求时，加上 `proxy_set_header X-Auth-Verify "1";`，并 `proxy_pass http://auth_shiny/`（打到根路径）。
- login.R 的 `ui(req)` 里判断 `req$HTTP_X_AUTH_VERIFY == "1"` 时只做鉴权：查 Cookie、查库，返回 200+头 `X-User` 或 401，不渲染页面；其余请求正常渲染登录/注册页。

这样鉴权请求和普通页面请求都走 Shiny 的根路径，仅靠 Header 区分。

### 2. 401 必须重定向到带尾斜杠的 `/login/`

**现象**：未登录时 Nginx 正确返回 302，但 `Location` 写的是 `/login`（无尾斜杠）。用户被重定向到 `http://localhost/login` 后，页面里的相对路径（如 `jquery-3.7.1/jquery.min.js`）会按「当前路径的目录」解析。对路径 `/login` 来说，目录被当作根 `/`，于是静态资源请求变成 `http://localhost/jquery-3.7.1/...`，命中 `location /` 被转到 **app_shiny**，导致 404 或返回甘特图 HTML，登录页无法加载、控件不渲染。

**解决**：`error_page 401` 的跳转目标必须是 **`/login/`**（带尾斜杠），例如：

```nginx
error_page 401 = @error401;
location @error401 {
    return 302 /login/;
}
```

并增加 `location = /login { return 302 /login/; }`，把误输入或旧链接的 `/login` 也统一 302 到 `/login/`。

### 3. `/login/` 的 location 必须能匹配到，且只打 auth_shiny

**现象**：浏览器访问 `http://localhost/login/` 时，有时看到的是「Snibe 甘特图」表头和图例，而不是登录页；F12 里还有 timevis、datatables 等主应用的资源 404。说明对 `/login/` 的请求被 Nginx 转到了 **app_shiny**，没进 auth_shiny。

**原因 1：location 顺序/优先级**  
业务用的 `location /` 会匹配所有路径。若没有专门、且优先级更高的「登录」location，`/login/` 会被 `location /` 接住，先走 `auth_request`；若鉴权因故返回 200（见下文），请求就会被 proxy 到 app_shiny，用户就看到甘特图。

**原因 2：Windows 下 default.conf 的 CRLF 换行（关键）**  
`default.conf` 在 Windows 上编辑或保存后常带 **CRLF**。该文件通过 volume 挂载进 Linux 容器后，Nginx 解析到的 location 路径可能带 `\r`（例如等价于 `"/login/\r"`），与真实请求 URI `"/login/"` 不一致，导致 **`location ^~ /login/` 等前缀永远匹配不上**，请求继续落到 `location /`，最终被转到 app_shiny。

**解决**：

- **先加精确匹配**，保证 `GET /login/` 一定进 auth_shiny、不落到 `location /`：
  ```nginx
  location = /login/ {
      proxy_pass http://auth_shiny/;
      # ... proxy_http_version、proxy_set_header 等
  }
  ```
  否则当正则因 CRLF 等原因未匹配时，`/login/` 会被 `location /` 接住；若鉴权返回 200（如仍有有效 Cookie），请求会被转发到 app_shiny，浏览器 URL 仍是 `/login/` 但拿到甘特图 HTML，相对路径静态资源变成 `/login/visjs-...` 等 → 404。
- 登录静态资源继续用**正则 location**，避免受行尾 `\r` 影响：
  ```nginx
  location ~ ^/login/(.*)$ {
      proxy_pass http://auth_shiny/$1$is_args$args;
      # ...
  }
  ```
  这样 `/login/xxx` 稳定匹配并转发到 auth_shiny（`/login/` 已由上面 `=` 处理）。
- 若仍异常，确保挂载进容器的 `default.conf` 为 **LF 换行**（编辑器里转为「Unix (LF)」后保存）。

### 4. Nginx 挂载的是「文件」不是「目录」

**现象**：`docker-compose up` 报错：`mount src=.../nginx/default.conf ... not a directory` 或类似。

**原因**：在 Windows 上若先写了 `volumes: - ./nginx/default.conf:...`，但 `./nginx` 或 `./nginx/default.conf` 实际不存在或误建成了目录，Docker 会按目录挂载导致失败。

**解决**：保证 `nginx/default.conf` 是**文件**且存在；若曾误建为目录，删掉该目录，从 WSL 或别处重新拷入**文件** `default.conf`，再 `docker-compose up -d`。

---

## 三、Shiny 登录应用 (login.R)

### 5. 鉴权请求要认 Header，不要认路径

见「二、1」：Shiny 不按 PATH_INFO 路由，所以鉴权逻辑不要写「若 path == "/verify" 则返回 200/401」，而要写「若请求头 `HTTP_X_AUTH_VERIFY == "1"` 则只做鉴权并返回 200/401」。

### 6. 登录成功后应跳转到主应用，不要停在「登录成功」页

**现象**：登录成功后仍停留在登录应用里的「登录成功，传递工号 xxx」页面，没有进入甘特图。

**解决**：在 login.R 里，校验通过后：

1. 用 `session$sendCustomMessage("set-cookie", list(name = "ivd_user", value = wid))` 写 Cookie。
2. 立即用 `session$sendCustomMessage("redirect", list(url = "/"))` 让浏览器跳转到根路径；前端需有对应 handler（如 `window.location.href = msg.url`）。
3. 删除「登录成功」的 UI 和 `logged_in` 模式及相关 `observeEvent(btn_logout)`，不再展示中间页。

### 7. 前端脚本里的引号与分号

**现象**：登录页能打开，但控制台报 `Uncaught SyntaxError: Unexpected token ';'`（例如在 login 页面某行），或 `Shiny is not defined`，导致控件无法交互、登录后不跳转。

**原因**：在 R 里用 `tags$script("...")` 写 Cookie 时，字符串里包含 `'; path=/'` 等，若与外层引号或 HTML 转义配合不当，可能被解析成错误语法；或 Shiny 对 script 做了转义导致异常。

**解决**：

- Cookie 的 path 部分拆成 JS 变量再拼接，例如：`var pathPart = '; path=/'; document.cookie = msg.name + '=' + v + pathPart;`
- 使用 `tags$script(HTML("..."))` 保证整段脚本按原样输出，避免被转义。
- redirect 的 CustomMessage 改为传对象：R 端 `list(url = "/")`，前端 `if (msg && msg.url) window.location.href = msg.url;`，避免字符串在序列化/编码时出问题。

### 8. 数据库未就绪时 auth_shiny 会反复退出

**现象**：auth_shiny 容器日志里反复出现 `connection to server at "db" ... failed: Connection refused` 或 `the database system is starting up`，然后进程退出，Nginx 报 502 或 auth_request 失败。

**原因**：login.R 在加载时（或首次请求时）就创建 `dbPool` 并可能立即用库；若 PostgreSQL 容器尚未完全启动，连接失败会导致 R 进程退出，Docker 再拉起重试，形成循环。

**解决**：属于启动顺序问题。可：

- 在 docker-compose 里为 auth-shiny 和 app-shiny 设 `depends_on: db`，并视情况加健康检查或延迟启动脚本；和/或
- 在 R 里对 pool 的创建与首次查询做 `tryCatch` 或重试，避免一失败就 exit；等 DB 就绪后，容器会稳定在 `Listening on http://0.0.0.0:3838`。

---

## 四、Docker 与构建

### 9. WSL 里构建时拉不到基础镜像（403 / 镜像源）

**现象**：在 WSL 执行 `docker compose -f docker-compose.login.yml build` 时报 `failed to resolve source metadata for ... ubuntu:22.04: 403 Forbidden` 或 `docker-credential-secretservice: ... libsecret-1.so.0: cannot open shared object file`。

**原因**：当前 Docker 配置的镜像源（如公司代理）拒绝访问，或环境缺少某库，导致无法拉取基础镜像。

**解决**：

- 若短期内无法改镜像源，可**不重建镜像**：在 docker-compose 里给 auth-shiny 服务加 volume，把宿主机上的 `login.R` 挂到容器内 `/app/app.R`，改完 R 后重启容器即可生效。
- 若必须构建，需在能访问外网或正确镜像源的环境（或修正 Docker 镜像配置）下 build，再导出/导入镜像。

### 10. Dockerfile 里 RUN 的引号与换行

**现象**：构建时报 `Syntax error: "(" unexpected` 等 shell 错误。

**原因**：`RUN` 里多行 `install.packages(...)` 或复杂命令时，引号、括号在 shell 解析时被错误拆分。

**解决**：把整条 R 命令写在一行，用 `&&` 连接，注意单双引号配对；必要时用 `\` 续行并保证每行引号闭合。

### 11. docker-compose 的 version 警告

**现象**：`the attribute version is obsolete, it will be ignored`。

**原因**：新版本 Docker Compose 不再使用顶层的 `version: "3.8"`。

**解决**：从 `docker-compose.yml` 和 `docker-compose.login.yml` 中删除 `version` 行即可，不影响功能。

---

## 五、排错与自检

### 12. 从网关容器里 curl 时要用对主机名

**现象**：在 `ivd_gateway` 里执行 `curl -i http://auth_shiny/login` 得到 `Empty reply from server` 或连接失败。

**原因**：Compose 里 upstream 名叫 `auth_shiny`，但该名字是 Nginx 内部用的；在**容器内**用 curl 时，应使用 Compose 服务名 **`auth-shiny-1`** 或 **`auth-shiny-2`**（带数字和连字符），并带端口，例如 `http://auth-shiny-1:3838/`。若写错主机名，可能请求到错误对象或无法解析。

### 13. PowerShell 里 curl 的引号与变量

**现象**：`docker exec ivd_gateway sh -c 'curl ... -w "%{http_code}" ...'` 报 `no URL specified` 或参数错乱。

**原因**：PowerShell 会对 `%`、`"` 等做解析，导致传入容器内的参数被截断或改写。

**解决**：避免在 `-w` 里用复杂格式；直接看响应头即可，例如：

```powershell
docker exec ivd_gateway curl -s -i -H "X-Auth-Verify: 1" http://auth_shiny/
```

看第一行是 `401` 还是 `200`。鉴权接口无 Cookie 时应返回 401。

### 14. 如何确认「先访问甘特图再被转到登录页」

**容易混淆**：

- 用户**直接**打开 `http://localhost/login/`：请求只命中 `location ~ ^/login/`，**不会**走 `auth_request`，会直接拿到登录页。
- 用户打开 `http://localhost/`：请求命中 `location /`，先 `auth_request`，无 Cookie 则 401 → 302 `/login/`，浏览器再请求 `/login/`，才看到登录页。

所以要验证「先甘特图再转登录」的流程，应**无痕访问 `http://localhost/`**，看是否先 302 到 `/login/`，再出现登录页；而不是用 curl 直接请求 `/login/`（那只会验证「登录页是否由 auth_shiny 提供」）。

---

## 六、配置与文件清单（当前推荐）

- **Nginx**：鉴权用 `location = /auth-verify` + `X-Auth-Verify: 1` + `proxy_pass http://auth_shiny/`；401 时 `return 302 /login/`；`/login` 重定向到 `/login/`；登录与静态资源用 **正则** `location ~ ^/login/(.*)$` + `proxy_pass http://auth_shiny/$1$is_args$args`；业务用 `location /` + `auth_request` + `proxy_pass http://app_shiny`。
- **login.R**：`ui(req)` 根据 `HTTP_X_AUTH_VERIFY == "1"` 做鉴权并返回 200/401；登录成功发 `set-cookie` 和 `redirect`（对象 `list(url = "/")`）；脚本用 `HTML()` 包裹并注意 Cookie path 的写法。
- **default.conf**：若在 Windows 上编辑，保存时使用 **LF 换行**，或依赖正则 location 减轻 CRLF 影响。

更细的逐行说明见 `nginx/default.conf` 内注释；自检命令见 `nginx/VERIFY.md`。

---

## 七、小结表

| 现象 | 可能原因 | 处理方向 |
|------|----------|----------|
| 访问 `/login/` 却看到甘特图 | location 未匹配到 /login/（如 CRLF、前缀写错） | 用正则 `~ ^/login/(.*)$`；保证 conf 为 LF |
| 登录页无控件 / Shiny is not defined | 静态资源 404（相对路径按 / 解析） | 401 重定向到 `/login/`；`/login` → 302 `/login/` |
| 鉴权一直 200、从不 302 到登录 | auth_shiny 未识别鉴权请求，返回了整页 HTML | 用 Header `X-Auth-Verify: 1`，不用路径 /verify |
| 登录成功仍停在登录页 | 未发 redirect 或前端未处理 | sendCustomMessage("redirect", list(url="/")) + 前端跳转 |
| auth_shiny 容器反复重启 | DB 未就绪，pool 连接失败 | depends_on + 重试/健康检查或 R 里 tryCatch |
| WSL build 403 / 拉不到镜像 | 镜像源或网络限制 | volume 挂载 login.R 替代重建；或换环境 build |
| curl 在网关内 no URL specified | PowerShell 吞引号/变量 | 用简单 `curl -s -i` 看状态行 |

以上为从尝试搭建 Nginx 到当前稳定部署过程中遇到的主要坑与对应处理，后续若再出现类似现象可先按本表与各节对照排查。

---

## 八、导出 schema.sql

在 **PowerShell** 中一条命令导出当前库的 schema（UTF-8，直接落盘到 Windows 目录，避免重定向乱码）。需先确保 Postgres 容器在跑（如本机 Docker 里 `ivd_postgres` 映射 5432），且把 `C:\IVD_Project` 换成你的项目目录。

```powershell
docker run --rm -e PGPASSWORD=mypassword -e PGCLIENTENCODING=UTF8 -v C:\IVD_Project:/out hub.rat.dev/library/postgres:15 pg_dump -h host.docker.internal -p 5432 -U myuser --schema-only --no-owner --no-privileges ivd_data -f /out/schema.sql
```

导出完成后，`schema.sql` 会出现在 `C:\IVD_Project\` 下。若库在 Rancher 等远程环境，将 `-h host.docker.internal` 改为该库的 host（或 IP），并保证运行命令的机器能访问其 5432 端口。

---

## 九、编译镜像并导出为 tar（拷到 Windows）

在项目根目录执行（WSL 或本机终端）。镜像 tag 以当前 `docker-compose.*.yml` 里的为准（如 `20260305`），可按需修改。

**登录鉴权镜像（auth_shiny，login.R）：**

```bash
docker compose -f docker-compose.login.yml build
docker save ivd_auth_shiny:20260305 -o ivd_auth_shiny.tar
cp ~/shiny_project/ivd_auth_shiny.tar /mnt/c/IVD_Project/
```

**业务 Shiny 镜像（甘特图 app.R）：**

```bash
docker compose -f docker-compose.shiny.yml build
docker save ivd_shiny:20260305 -o ivd_shiny.tar
cp ~/shiny_project/ivd_shiny.tar /mnt/c/IVD_Project/
```

若当前不在 `~/shiny_project`，把 `cp` 里的路径改成项目实际路径；`/mnt/c/IVD_Project/` 换成你要放 tar 的 Windows 目录。在目标环境用 `docker load -i ivd_auth_shiny.tar` 或 `docker load -i ivd_shiny.tar` 导入即可。
