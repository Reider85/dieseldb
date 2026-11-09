#!/bin/bash

# Self-Hosted Runner Setup Script
# Configures a local runner for DieselDB CI/CD fallback

set -e

echo "=== DieselDB Self-Hosted Runner Setup ==="

# Configuration
RUNNER_NAME="dieseldb-local-runner"
RUNNER_WORKDIR="./runner-workdir"
GITHUB_PAT="${GITHUB_PAT:-}"  # Personal Access Token with repo scope
REPO_URL="https://github.com/Reider85/dieseldb"

# Check if GitHub PAT is provided
if [ -z "$GITHUB_PAT" ]; then
    echo "❌ Error: GITHUB_PAT environment variable is required"
    echo "Please set your GitHub Personal Access Token:"
    echo "export GITHUB_PAT=ghp_your_token_here"
    exit 1
fi

# Check if runner is already configured
if [ -d "$RUNNER_WORKDIR" ] && [ -f "$RUNNER_WORKDIR/.runner" ]; then
    echo "✅ Runner already configured at $RUNNER_WORKDIR"
    echo "To remove and reconfigure:"
    echo "  rm -rf $RUNNER_WORKDIR"
    echo "  $0"
    exit 0
fi

# Create runner workdir
echo "Creating runner workdir..."
mkdir -p "$RUNNER_WORKDIR"
cd "$RUNNER_WORKDIR"

# Download and configure runner
echo "Downloading GitHub Actions runner..."
curl -o runner.tar.gz -L https://github.com/actions/runner/releases/download/v2.319.1/actions-runner-linux-x64-2.319.1.tar.gz

echo "Extracting runner..."
tar xzf runner.tar.gz
rm runner.tar.gz

# Configure runner
echo "Configuring runner..."
./config.sh --url "$REPO_URL" --token "$GITHUB_PAT" --name "$RUNNER_NAME" --work ./_work

# Create systemd service for auto-start
echo "Creating systemd service..."
cat > /tmp/dieseldb-runner.service << EOF
[Unit]
Description=DieselDB Self-Hosted GitHub Actions Runner
After=network.target

[Service]
User=$USER
WorkingDirectory=$PWD/$RUNNER_WORKDIR
ExecStart=$PWD/$RUNNER_WORKDIR/run.sh
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

# Install systemd service
echo "Installing systemd service..."
sudo cp /tmp/dieseldb-runner.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable dieseldb-runner.service
sudo systemctl start dieseldb-runner.service

echo "✅ Self-hosted runner setup complete!"
echo ""
echo "Runner Status:"
sudo systemctl status dieseldb-runner.service
echo ""
echo "To view runner logs:"
echo "  sudo journalctl -u dieseldb-runner.service -f"
echo ""
echo "To stop the runner:"
echo "  sudo systemctl stop dieseldb-runner.service"
echo ""
echo "To remove the runner:"
echo "  sudo systemctl stop dieseldb-runner.service"
echo "  sudo systemctl disable dieseldb-runner.service"
echo "  sudo rm /etc/systemd/system/dieseldb-runner.service"
echo "  rm -rf $RUNNER_WORKDIR"