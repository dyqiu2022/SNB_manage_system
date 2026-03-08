# Docker Hub 国内镜像加速（不需翻墙）

在 **WSL Ubuntu** 里执行：

```bash
# 1. 配置国内镜像（多个源，Docker 会依次尝试）
sudo mkdir -p /etc/docker
sudo tee /etc/docker/daemon.json <<'EOF'
{
  "registry-mirrors": [
    "https://docker.xuanyuan.me",
    "https://docker.m.daocloud.io",
    "https://docker.1panel.live",
    "https://dockerhub.icu"
  ]
}
EOF

# 2. 重启 Docker
sudo service docker restart

# 3. 再构建
cd /home/joey/shiny_project
docker compose -f docker-compose.shiny.yml build shiny
```

若某个源不可用，可删除该行或换其他国内源（如 https://docker.unsee.tech）。国内镜像站会变动，建议定期查最新列表。
