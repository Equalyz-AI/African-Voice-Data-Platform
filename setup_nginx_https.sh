#!/bin/bash
set -e

# Prompt user for input
echo "🔧 Setting up Nginx with SSL for your server..."
read -p "Enter your server's IP address [51.20.115.13]: " IP
read -p "Enter your subdomains (space-separated, e.g., aiguide.lagosstate.gov.ng api.aiguide.lagosstate.gov.ng): " DOMAINS
read -p "Enter your email address for Let's Encrypt notifications: " EMAIL

echo "✅ IP set to: $IP"
echo "✅ Domains: $DOMAINS"

# Convert DOMAINS string into array
DOMAIN_ARRAY=($DOMAINS)

# DNS check for each domain
for DOMAIN in "${DOMAIN_ARRAY[@]}"; do
    echo "🔍 Checking DNS for $DOMAIN..."
    RESULT=$(nslookup $DOMAIN | grep 'Address:' | tail -n1 | awk '{print $2}')
    if [ "$RESULT" != "$IP" ]; then
        echo "❌ $DOMAIN is not pointing to $IP. Found: $RESULT"
    else
        echo "✅ $DOMAIN correctly points to $IP"
    fi
done

# Set app name from current directory
APP_NAME=$(basename "$PWD")
NGINX_DIR="/etc/nginx/conf.d"

echo "🔧 Creating Nginx configs for each domain..."

for DOMAIN in "${DOMAIN_ARRAY[@]}"; do
    NGINX_SITE="$NGINX_DIR/$APP_NAME-$DOMAIN.conf"

    sudo tee $NGINX_SITE > /dev/null <<EOF
server {
    listen 80;
    server_name $DOMAIN;

    location / {
        proxy_pass http://localhost:8000;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
    }
}
EOF

done

# Test and reload Nginx
sudo nginx -t && sudo systemctl reload nginx
echo "✅ Nginx configured for all domains"

# Install Certbot
echo "📦 Installing Certbot and Nginx plugin..."
sudo apt update
sudo apt install -y certbot python3-certbot-nginx

# Request SSL certificates
for DOMAIN in "${DOMAIN_ARRAY[@]}"; do
    echo "🔐 Requesting SSL certificate for $DOMAIN..."
    sudo certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos --email "$EMAIL"
done

# Test auto-renewal
echo "🔁 Testing certificate auto-renewal..."
sudo certbot renew --dry-run

echo "🎉 Success! Your domains are now secured with HTTPS."
(venv) root@srv1165987:~/lagosgenai#













