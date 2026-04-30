# IVD 项目 Docker 镜像编译与部署 — 入门指南

> 本文档面向零 Docker 基础的同事，用 IVD 项目中 \*\*app.R 的镜像编译\*\* 作为唯一案例，帮助你理解：
> - 什么是 Docker 镜像、容器
> - Dockerfile 怎么写、每行什么意思
> - docker-compose.yml 怎么写、每行什么意思
> - 编译好的镜像怎么部署到运行环境

\---

## 一、我们的环境长什么样

我们有两台电脑（或两个环境），分工不同：

```
┌─────────────────────────────────┐         ┌─────────────────────────────────────┐
│  开发电脑（Windows + WSL）      │         │  运行服务器（Windows Docker Desktop）│
│                                 │         │                                     │
│  你平时写代码的地方              │        │  系统实际运行的地方                  │
│                                 │         │                                     │
│  ┌───────────────────────────┐  │         │  ┌───────────────────────────────┐  │
│  │ WSL (Ubuntu 子系统)       │ │         │  │ Docker Desktop                │  │
│  │                           │  │         │  │                               │  │
│  │  源代码: app.R, R/\*.R     │  │        │  │  ┌──────────────────┐ │  │
│  │  编译工具: docker build   │  │        │  │  │  容器（Container）      │ │  │
│  │  导出工具: docker save    │  │        │  │  │                        │ │  │
│  │                           │  │        │  │  │  你的 app.R 就跑在这里  │ │  │
│  └────────────────────┘  │        │  │  │  端口 3838              │ │  │
│                                 │         │  │  │                         │  │  │
│  编译产物:                       │         │  │  └─────────────────┘ │  │
│  ivd\_shiny\_2026\_04\_27.tar ──────┼── cp ──►│  │                               │  │
│                                 │         │  │  ┌─────────────────────────┐ │  │
│                                 │         │  │  │  PostgreSQL 数据库    │ │ │
│                                 │         │  │  │  端口 5432            │ │  │
│                                 │         │  │  └─────────────────────────┘ │  │
│                                 │         │  └───────────────────────────────┘  │
└─────────────────────────────────┘         └─────────────────────────────────────┘

流程：WSL 里编译镜像 → 导出为 .tar 文件 → 拷贝到 Windows → Windows Docker 加载运行
```

**为什么要分两步？**

* WSL 里有完整的 Linux 环境，可以直接用 `docker build` 编译镜像
* 但 WSL 里的 Docker 不适合跑长期服务（重启 WSL 容器就没了）
* 所以我们把编译好的镜像"打包"搬过去，在 Windows Docker Desktop 里稳定运行

**三个核心概念（1 分钟理解）：**

|概念|类比|说明|
|-|-|-|
|**Dockerfile**|菜谱|一份文本文件，描述"怎么把 app.R 做成一个可运行的包"|
|**镜像（Image）**|做好的菜|按照 Dockerfile 编译出来的产物，是一个只读的"安装包"|
|**容器（Container）**|上菜开吃|从镜像启动的运行实例，就是你的 app.R 实际跑起来的进程|

\---

## 二、系统架构层级图

下面这张图展示了一个 app.R 从"源代码"变成"正在运行的服务"的完整层级：

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Windows 电脑                                   │
│                                                                         │
│  ┌─ 第 1 层：浏览器 ────────────────────────────────────┐  │
│  │  用户访问 http://localhost/gantt/                                │  │
│  │  浏览器发送 HTTP 请求                                             │  │
│  └───────────────────────────────────────────────────────────────────┘  │
│         │                                                               │
│         ▼                                                               │
│  ┌─ 第 2 层：Rancher Desktop（容器运行平台）───────────────────┐  │
│  │                                                                   │  │
│  │   ┌─ 容器 A：app-shiny ────────────────────────────┐   │  │
│  │   │                                                          │   │  │
│  │   │  ┌──────────────────────────────────────────────────┐    │   │  │
│  │   │  │  Ubuntu 22.04（精简版 Linux）                   │    │   │  │
│  │   │  │                                                  │    │   │  │
│  │   │  │  ┌────────────────────────────────────────────┐  │    │   │  │
│  │   │  │  │  R 语言环境 + 10 个 R 包                  │   │    │   │  │
│  │   │  │  │  (shiny, dplyr, DBI, RPostgres, timevis…)  │   │    │   │  │
│  │   │  │  │                                            │   │    │   │  │
│  │   │  │  │  ┌──────────────────────────────────────┐  │   │    │   │  │
│ │   │  │  │  │  app.R（你的业务代码）              │  │   │   │   │  │
│ │   │  │  │  │  + R/ivd\_server\_helpers.R（辅助函数）│ │   │   │   │  │
│  │   │  │  │  └──────────────────────────────────────┘  │   │    │   │  │
│  │   │  │  └────────────────────────────────────────────┘   │    │   │  │
│  │   │  └──────────────────────────────────────────────────┘    │    │  │
│  │   │                                                          │    │  │
│  │   │  端口: 3838                                             │   │  │
│  │   └──────────────────────────────────────────────────────────┘   │  │
│  │                                                                  │  │
│  │   ┌─ 容器 B：PostgreSQL ──────────────────────────┐   │  │
│  │   │  数据库，存储所有业务数据                               │   │  │
│  │   │  端口: 5432                                            │   │  │
│  │   └─────────────────────────────────────────────────────────┘   │  │
│  │                                                                   │  │
│  │   容器 A 和 B 通过 Docker 内部网络（ivd\_network）互相访问          │  │
│  │                                                                   │  │
│  └───────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
```

**层级关系：** 容器就像一个"精简的虚拟机"，从外到内依次是：

1. 操作系统层（Ubuntu 22.04）
2. 运行环境层（R 语言 + 各种包）
3. 业务代码层（app.R）

每一层都是上一层的基础上"叠加"的 —— 这就是 Docker 的**分层镜像**思想。

\---

## 三、Dockerfile 逐行解读

Dockerfile 就是"菜谱"，告诉 Docker 怎么一步步把你的 app.R 打包成镜像。

### 3.1 完整构建版（Dockerfile）

这个文件用于**从零开始**构建一个完整的镜像。平时第一次部署时用一次，后续不需要再跑。

```dockerfile
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第 1 行：选择基础操作系统
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
FROM ubuntu:22.04
#  ▲ 使用 Ubuntu 22.04 作为基础镜像
#  为什么不选别的？因为 Ubuntu 22.04（jammy）对 R 语言支持好，
#  CRAN 官方提供了对应的软件源，装 R 包很方便。

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第 3-4 行：设置环境变量
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ENV DEBIAN\_FRONTEND=noninteractive
#  ▲ 告诉 apt-get "别弹对话框问我问题，默认选 Yes 就行"
#  Docker 编译过程是全自动的，没有人在屏幕前点按钮，所以必须禁用交互。

ENV LANG=C.UTF-8
#  ▲ 设置字符编码为 UTF-8
#  确保 R 包安装过程中不会因为中文字符出问题。

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第 6-22 行：安装 R 语言环境（一大坨，但逻辑很清晰）
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
RUN apt-get update -qq \\
#  ▲ 更新 Ubuntu 的软件包列表（-qq 安静模式，少输出日志）
    \&\& apt-get install -y --no-install-recommends \\
#  ▲ 安装下面列出的软件包（-y 自动确认，--no-install-recommends 不装推荐的包，减小镜像体积）
        ca-certificates \\
#  ▲ SSL 证书，让 wget 能访问 HTTPS 网站（下载 R 的 GPG 公钥需要）
        dirmngr \\
        gnupg \\
#  ▲ GPG 密钥管理工具，用来验证 CRAN 软件源的签名（确保下载的 R 是正版）
        libpq-dev \\
#  ▲ PostgreSQL 的 C 语言客户端库（RPostgres 包需要它来连接数据库）
        software-properties-common \\
#  ▲ 提供添加第三方软件源的能力
        wget \\
#  ▲ 下载工具，用来从网上拉文件
    \&\& wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter\_pubkey.asc \\
        | tee /etc/apt/trusted.gpg.d/cran\_ubuntu\_key.asc > /dev/null \\
#  ▲ 下载 CRAN 官方的 GPG 公钥并注册到系统
#  这样 apt-get 就信任 CRAN 源的软件包了
    \&\& echo "deb https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/" \\
        > /etc/apt/sources.list.d/cran.list \\
#  ▲ 把 CRAN 软件源地址写入系统配置
#  jammy-cran40 = Ubuntu 22.04 的 R 4.x 版本源
    \&\& apt-get update -qq \\
#  ▲ 再次更新软件包列表（这次会包含 CRAN 源里的 R）
    \&\& apt-get install -y --no-install-recommends r-base r-base-dev \\
#  ▲ 安装 R 语言本体（r-base）和开发工具（r-base-dev，安装 R 包时需要编译）
    \&\& rm -rf /var/lib/apt/lists/\*
#  ▲ 清理 apt 缓存，减小镜像体积（这一行很重要，能省几百 MB）

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第 24-28 行：安装 R 包
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
ENV R\_LIBS\_SITE=/usr/local/lib/R/site-library
#  ▲ 指定 R 包的安装路径
#  ⚠️ 这一行非常关键！如果不指定，R 包默认装到 /root/ 下，
#  后面 COPY app.R 时不会覆盖 /root/，导致运行时找不到包。
#  指定到 /usr/local/lib/ 下，所有用户都能访问。

RUN mkdir -p /usr/local/lib/R/site-library \&\& \\
#  ▲ 创建上面指定的目录
  R -e "install.packages(c('shiny','dplyr','tibble','tidyr','DBI','RPostgres','DT','timevis','pool','digest'), \\
       lib = '/usr/local/lib/R/site-library', \\
       repos = 'https://cloud.r-project.org/', \\
       dependencies = TRUE)" \&\& \\
#  ▲ 安装 10 个 R 包，每个的作用见下表：
#
#  ┌────────────┬──────────────────────────────────────────┐
#  │ 包名       │ 作用                                      │
#  ├────────────┼──────────────────────────────────────────┤
#  │ shiny      │ Web 框架，把 R 变成网页应用               │
#  │ dplyr      │ 数据操作（筛选、分组、汇总）               │
#  │ tibble     │ 现代数据框，比 data.frame 更好用          │
#  │ tidyr      │ 数据整理（宽表↔长表转换）                 │
#  │ DBI        │ 数据库通用接口                            │
#  │ RPostgres  │ PostgreSQL 驱动，让 R 能连 PG 数据库      │
#  │ DT         │ 数据表格展示（可排序、搜索的 HTML 表格）   │
#  │ timevis    │ 甘特图可视化（vis-timeline 的 R 封装）     │
#  │ pool       │ 数据库连接池（自动管理连接的创建和释放）    │
#  │ digest     │ 哈希加密（用于密码校验）                   │
#  └────────────┴──────────────────────────────────────────┘
#
#  dependencies = TRUE 表示连带安装这些包的依赖

  R -e "stopifnot(requireNamespace('shiny', quietly = TRUE, lib.loc = '/usr/local/lib/R/site-library'))"
#  ▲ 验证 shiny 包是否安装成功（如果失败，编译直接报错，不会生成坏镜像）

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 第 30-35 行：放入业务代码并启动
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
COPY app.R /app/app.R
#  ▲ 把本地的 app.R 文件复制到镜像里的 /app/ 目录
COPY R/ /app/R/
#  ▲ 把本地的 R/ 目录（辅助函数库）复制到镜像里的 /app/R/ 目录
WORKDIR /app
#  ▲ 设置工作目录为 /app（后面的命令都在这个目录下执行）
EXPOSE 3838
#  ▲ 声明容器对外暴露 3838 端口（Shiny 的默认端口）
#  注意：这只是声明，实际映射到宿主机哪个端口由 docker-compose.yml 决定
CMD \["R", "-e", "shiny::runApp('/app/app.R', host = '0.0.0.0', port = 3838)"]
#  ▲ 容器启动时执行的命令：用 R 运行 app.R
#  host = '0.0.0.0' 表示监听所有网络接口（不限制只本机访问）
#  port = 3838 监听 3838 端口
```

### 3.2 增量更新版（Dockerfile.update）

日常开发中最常用的文件。修改了 app.R 之后，用这个文件快速更新镜像：

```dockerfile
FROM ivd\_shiny:20260305
#  ▲ 基于之前完整构建的镜像（ivd\_shiny:20260305）
#  这个镜像里已经有了 Ubuntu + R + 10 个包，不用重新安装！
#  相当于"在做好的菜上换一个配料"，不用从头做。

COPY app.R /app/app.R
#  ▲ 只替换 app.R 文件（覆盖基础镜像里的旧版本）
COPY R/ /app/R/
#  ▲ 只替换 R/ 目录（覆盖基础镜像里的旧版本）

CMD \["R", "-e", "shiny::runApp('/app/app.R', host = '0.0.0.0', port = 3838)"]
#  ▲ 启动命令，和完整版一模一样
```

**对比：**

||完整构建（Dockerfile）|增量更新（Dockerfile.update）|
|-|-|-|
|基础|`FROM ubuntu:22.04`（从零开始）|`FROM ivd\_shiny:20260305`（从已有镜像开始）|
|需要装 R 吗|需要，要装 10 分钟以上|不需要，基础镜像里已经有了|
|需要装 R 包吗|需要，也要很久|不需要|
|只做什么|全部从头来|只替换 app.R 和 R/ 目录|
|编译时间|30-45 分钟|1-2 分钟|
|什么时候用|第一次部署，或 R 包版本要升级|日常改了 app.R 后重新部署|

\---

## 四、docker-compose.yml 逐行解读

docker-compose.yml 的作用是**一次性定义和启动多个容器**。下面是一个简化版（只保留核心部分）：

```yaml
# docker-compose.yml 定义了"哪些容器一起跑、怎么互相连接"
# 就像一份餐厅排班表：谁做菜、谁端盘子、谁收银

services:
#  ▲ 所有容器都定义在 services 下面

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # 服务 1：PostgreSQL 数据库
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  db:
    image: hub.rat.dev/library/postgres:15
    #  ▲ 使用 PostgreSQL 15 镜像
    #  hub.rat.dev 是国内 Docker Hub 镜像站，下载速度比官方源快

    container\_name: ivd\_postgres
    #  ▲ 容器名字叫 ivd\_postgres（方便用 docker logs 等命令找到它）

    restart: always
    #  ▲ 容器挂了自动重启（比如电脑重启后数据库也会自动起来）

    environment:
    #  ▲ 环境变量，相当于给数据库传参数
      POSTGRES\_USER: myuser
      #  ▲ 数据库用户名
      POSTGRES\_PASSWORD: mypassword
      #  ▲ 数据库密码
      POSTGRES\_DB: ivd\_data
      #  ▲ 默认创建的数据库名

    ports:
      - "5432:5432"
      #  ▲ 端口映射："宿主机端口:容器端口"
      #  把容器内的 5432 映射到宿主机的 5432
      #  这样外部工具也能连这个数据库

    volumes:
      - ./postgres\_data:/var/lib/postgresql/data
      #  ▲ 数据持久化：把容器内的数据库文件目录挂载到宿主机
      #  这样即使删掉容器重建，数据也不会丢

    networks:
      - ivd\_net
      #  ▲ 加入 ivd\_net 网络（后面定义的）

  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  # 服务 2：Shiny 应用（你的 app.R 跑在这里）
  # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  app-shiny:
    image: ivd\_shiny:2026\_04\_27\_10\_30
    #  ▲ 使用前面用 Dockerfile 编译出来的镜像
    #  注意 tag 是时间戳格式：年\_月\_日\_时\_分
    #  每次部署用新的时间戳，不会覆盖旧版本

    restart: always
    #  ▲ 挂了自动重启

    environment:
    #  ▲ 告诉 app.R 怎么连数据库
      PG\_HOST: db
      #  ▲ 数据库地址填的是 "db"（不是 IP！）
      #  Docker Compose 会自动把 "db" 解析为 PostgreSQL 容器的 IP
      #  这就是"服务发现"——容器之间通过服务名互访
      PG\_PORT: "5432"
      #  ▲ 数据库端口
      PG\_DBNAME: ivd\_data
      #  ▲ 数据库名
      PG\_USER: myuser
      #  ▲ 数据库用户名
      PG\_PASSWORD: mypassword
      #  ▲ 数据库密码

    depends\_on:
      - db
      #  ▲ 启动顺序：先启动 db，再启动 app-shiny
      #  否则 app-shiny 启动时数据库还没准备好会报错

    networks:
      - ivd\_net
      #  ▲ 和 db 在同一个网络里，才能通过 "db" 这个名字互访

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# 网络定义
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
networks:
  ivd\_net:
    name: ivd\_network
    #  ▲ 创建一个自定义网络叫 ivd\_network
    #  加入这个网络的所有容器可以通过"服务名"互相访问
    #  比如 app-shiny 容器里 ping db 就能通
```

**docker-compose.yml 的本质：** 把 `docker run` 命令的各种参数（端口映射、环境变量、网络、启动顺序）写成一份配置文件，一条 `docker compose up` 命令就能全部启动。

\---

## 五、编译与部署完整流程

### 5.1 两种编译场景

||场景 A：初次编译|场景 B：日常更新|
|-|-|-|
|**什么时候用**|第一次部署 / 需要升级 R 包|只改了 app.R 或 R/\*.R|
|**用哪个 Dockerfile**|`Dockerfile`（完整构建）|`Dockerfile.update`（增量构建）|
|**做了什么**|从 Ubuntu 开始，安装 R + 10 个包 + 复制代码|只替换 app.R 和 R/ 目录|
|**编译时间**|30-45 分钟|1-2 分钟|

### 5.2 完整部署步骤

以下 8 步在两个场景中只有第 2 步不同，其余完全一样：

**第 1 步：生成时间戳标签**（WSL 编译端）

```bash
TAG=$(date +%Y\_%m\_%d\_%H\_%M)
echo $TAG
# 输出类似：2026\_04\_27\_14\_30
```

**第 2 步：编译镜像**（WSL 编译端）

```bash
cd /home/joey/shiny\_project

# 场景 A（初次编译，30-45 分钟）：
docker build --no-cache -f Dockerfile -t ivd\_shiny:$TAG .

# 场景 B（日常更新，1-2 分钟）：
docker build --no-cache -f Dockerfile.update -t ivd\_shiny:$TAG .
```

> - `--no-cache`：不用缓存，确保用最新的 app.R
> - `-f Dockerfile`：指定用哪个 Dockerfile
> - `-t ivd\_shiny:$TAG`：给镜像起名并打上时间戳标签
> - `.`：在当前目录找 Dockerfile 和 COPY 的文件

**第 3 步：导出镜像为 .tar 文件**（WSL 编译端）

```bash
docker save ivd\_shiny:$TAG -o /tmp/ivd\_shiny\_$TAG.tar
```

> 把镜像打包成一个 `.tar` 文件，通常 500MB-1.5GB。就像把做好的菜装进饭盒。

**第 4 步：拷贝到 Windows 目录**（WSL 编译端）

```bash
cp /tmp/ivd\_shiny\_$TAG.tar /mnt/c/IVD\_Project/
```

> `/mnt/c/` 就是 Windows 的 C 盘，WSL 可以直接访问。

**第 5 步：导入镜像**（Windows 运行端）

```bash
docker.exe load -i C:\\\\IVD\_Project\\\\ivd\_shiny\_$TAG.tar
```

> 从 .tar 文件加载镜像到 Windows Docker Desktop。加载后用 `docker.exe images` 可以看到新镜像。

**第 6 步：更新 docker-compose.yml 中的镜像标签**（Windows 运行端）

把 `image: ivd\_shiny:旧tag` 改为 `image: ivd\_shiny:新tag`

**第 7 步：重启容器**（Windows 运行端）

```bash
docker.exe compose -f C:\\\\IVD\_Project\\\\docker-compose.yml up -d --force-recreate app-shiny
```

**第 8 步：检查日志**（Windows 运行端）

```bash
sleep 8
docker.exe logs ivd\_project-app-shiny-1-1 --tail 20
# 看到 "Listening on http://0.0.0.0:3838" 就是成功了
```

\---

## 六、小结

```
  Dockerfile          docker-compose.yml         实际操作命令
  ┌─────────┐        ┌─────────────────┐        ┌──────────────────┐
  │ 怎么打包  │        │ 怎么编排多个容器  │        │ 怎么编译和部署    │
  │ 一个应用  │        │ 之间的关系      │        │                  │
  │         │        │                 │        │ docker build     │
  │ FROM... │        │ services:       │        │ docker save      │
  │ RUN...  │        │   db: ...       │        │ cp               │
  │ COPY... │        │   app-shiny: ...│        │ docker.exe load  │
  │ CMD...  │        │ networks: ...   │        │ docker compose up│
  └─────────┘        └─────────────────┘        └──────────────────┘

  Dockerfile 回答：    compose 回答：              你需要执行的：
  "镜像是怎么做的"     "容器之间怎么配合"           "怎么让代码上线"
```

记住这三个文件的职责分工，Docker 就没那么神秘了。

