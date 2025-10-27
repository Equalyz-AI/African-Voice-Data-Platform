#!/bin/bash

set -e

# Automatically detect project directory and name
APP_DIR=$(pwd)
APP_NAME=$(basename "$APP_DIR")
ENV_FILE="$APP_DIR/.env"
DOCKER_COMPOSE_FILE="$APP_DIR/docker-compose.yml"
SYSTEMD_FILE="/etc/systemd/system/${APP_NAME}.service"
NGINX_SITE="/etc/nginx/sites-available/${APP_NAME}"
NGINX_SITE_LINK="/etc/nginx/sites-enabled/${APP_NAME}"


DOMAIN_OR_IP="16.170.230.37"
# Ask for the domain and set it to the variable DOMAIN_OR_IP
read -p "Enter your domain or IP address (e.g., api.eko360.ai or 16.171.30.215):  (default: $DOMAIN_OR_IP): " input_domain
if [ -n "$input_domain" ]; then
    DOMAIN_OR_IP="$input_domain"
fi

# Ask for the app name and set it to the variable APP_NAME
read -p "Enter your app name (default: $APP_NAME): " input_app_name
if [ -n "$input_app_name" ]; then
    APP_NAME="$input_app_name"
fi

echo "🛠 Using APP_NAME=$APP_NAME"
echo "📁 Working from APP_DIR=$APP_DIR"
echo "🌐 Configuring for DOMAIN_OR_IP=$DOMAIN_OR_IP"

# 1. System update and install dependencies
echo "🔄 Updating system and installing Docker, Docker Compose, and Nginx..."
sudo apt update && sudo apt upgrade -y
sudo apt install -y nginx ca-certificates curl gnupg

# Remove broken/partial Docker installs
sudo apt remove -y containerd.io docker-ce docker-ce-cli docker-compose-plugin || true
sudo apt autoremove -y
sudo apt update

# Docker setup
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | \
  sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu \
  $(lsb_release -cs) stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null

sudo apt update
sudo apt install -y docker-ce docker-ce-cli docker-buildx-plugin docker-compose-plugin

# 2. Add user to docker group
sudo usermod -aG docker $USER

# 3. Enable Docker to start on boot
sudo systemctl enable docker

# 4. Create Nginx configuration
echo "🌐 Creating Nginx config for $DOMAIN_OR_IP..."
sudo tee $NGINX_SITE > /dev/null <<EOF
server {
    listen 80;
    server_name $DOMAIN_OR_IP;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

# 5. Enable and reload Nginx
sudo ln -sf $NGINX_SITE $NGINX_SITE_LINK
sudo nginx -t && sudo systemctl reload nginx
sudo systemctl enable nginx

# 6. Build Docker containers
echo "🐳 Building Docker containers..."
cd "$APP_DIR"
sudo docker compose down || true
sudo docker compose build
sudo docker compose up -d

# 7. Create systemd service
echo "🔧 Creating systemd service..."
sudo tee "$SYSTEMD_FILE" > /dev/null <<EOF
[Unit]
Description=Docker Compose FastAPI Service for $APP_NAME
Requires=docker.service
After=docker.service

[Service]
WorkingDirectory=$APP_DIR
ExecStart=/usr/bin/docker compose up
ExecStop=/usr/bin/docker compose down
Restart=always
User=ubuntu
Environment=DOTENV_PATH=$ENV_FILE

[Install]
WantedBy=multi-user.target
EOF

# 8. Reload systemd
sudo systemctl daemon-reexec
sudo systemctl daemon-reload
sudo systemctl enable "$APP_NAME"
sudo systemctl restart "$APP_NAME"

echo "✅ Deployment complete! App is running at http://$DOMAIN_OR_IP"
