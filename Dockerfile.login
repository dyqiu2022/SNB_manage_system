FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive
ENV LANG=C.UTF-8

# 基础 R 环境
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

# 安装 R 包（与主应用保持一致）
ENV R_LIBS_SITE=/usr/local/lib/R/site-library
RUN mkdir -p /usr/local/lib/R/site-library && \
  R -e "install.packages(c('shiny','dplyr','tibble','tidyr','DBI','RPostgres','DT','timevis','pool','digest'), lib = '/usr/local/lib/R/site-library', repos = 'https://cloud.r-project.org/', dependencies = TRUE)" && \
  R -e "stopifnot(requireNamespace('shiny', quietly = TRUE, lib.loc = '/usr/local/lib/R/site-library'))"

COPY login.R /app/app.R
WORKDIR /app
EXPOSE 3838

CMD ["R", "-e", "shiny::runApp('/app/app.R', host = '0.0.0.0', port = 3838)"]

