# 不依赖 rocker 镜像：从 Ubuntu 安装 R + Shiny（避免 Docker Hub 拉取超时）
FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8

# 安装 R 及 CRAN 源（Ubuntu 22.04 jammy）
RUN apt-get update -qq \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        dirmngr \
        gnupg \
        libpq-dev \
        software-properties-common \
        wget \
    && wget -qO- https://cloud.r-project.org/bin/linux/ubuntu/marutter_pubkey.asc \
        | tee /etc/apt/trusted.gpg.d/cran_ubuntu_key.asc > /dev/null \
    && echo "deb https://cloud.r-project.org/bin/linux/ubuntu jammy-cran40/" \
        > /etc/apt/sources.list.d/cran.list \
    && apt-get update -qq \
    && apt-get install -y --no-install-recommends r-base r-base-dev \
    && rm -rf /var/lib/apt/lists/*

# 安装 R 包到系统库路径（避免 build 时装到 /root 下导致运行时找不到）
ENV R_LIBS_SITE=/usr/local/lib/R/site-library
RUN mkdir -p /usr/local/lib/R/site-library && \
  R -e "install.packages(c('shiny','dplyr','tibble','tidyr','DBI','RPostgres','DT','timevis','pool','digest'), lib = '/usr/local/lib/R/site-library', repos = 'https://cloud.r-project.org/', dependencies = TRUE)" && \
  R -e "stopifnot(requireNamespace('shiny', quietly = TRUE, lib.loc = '/usr/local/lib/R/site-library'))"

COPY app.R /app/app.R
COPY R/ /app/R/
WORKDIR /app
EXPOSE 3838

CMD ["R", "-e", "shiny::runApp('/app/app.R', host = '0.0.0.0', port = 3838)"]
