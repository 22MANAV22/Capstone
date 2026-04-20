#!/bin/bash

################################################################################
# AIRFLOW ON EC2 - AUTOMATED SETUP SCRIPT
# Version: 2.0 - Production Ready
# Execution: One command setup - everything automated!
# Time: ~5-10 minutes total
################################################################################

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

section_header() {
    echo ""
    echo "======================================================================"
    echo "  $1"
    echo "======================================================================"
    echo ""
}

################################################################################
# SECTION 1: CONFIGURATION
################################################################################

section_header "STEP 1: CONFIGURATION"

# These MUST be updated - ask user for input
read -p "Enter your AWS S3 bucket name (e.g., capstone-ecomm-yourname): " BUCKET_NAME
read -p "Enter your Databricks workspace URL (e.g., https://dbc-xxxxx.cloud.databricks.com): " DATABRICKS_URL
read -p "Enter your Databricks PAT token: " DATABRICKS_TOKEN
read -p "Enter your AWS Access Key ID: " AWS_ACCESS_KEY
read -sp "Enter your AWS Secret Access Key: " AWS_SECRET_KEY
echo ""
read -p "Enter your SNS Topic ARN (or press Enter to skip): " SNS_ARN

# Validate inputs
if [ -z "$BUCKET_NAME" ] || [ -z "$DATABRICKS_URL" ] || [ -z "$DATABRICKS_TOKEN" ] || [ -z "$AWS_ACCESS_KEY" ] || [ -z "$AWS_SECRET_KEY" ]; then
    log_error "Missing required configuration!"
    exit 1
fi

log_success "Configuration captured"

# Set environment variables
export AIRFLOW_HOME=~/airflow
export AIRFLOW_UID=1000

log_success "Environment variables set"
echo "  AIRFLOW_HOME: $AIRFLOW_HOME"
echo "  Bucket: $BUCKET_NAME"
echo "  Databricks: $DATABRICKS_URL"

################################################################################
# SECTION 2: SYSTEM UPDATES
################################################################################

section_header "STEP 2: SYSTEM UPDATES & DEPENDENCIES"

log_info "Updating package lists..."
sudo apt-get update -qq
sudo apt-get upgrade -y -qq

log_info "Installing system dependencies..."
sudo apt-get install -y -qq \
    python3-pip \
    python3-venv \
    python3-dev \
    build-essential \
    libpq-dev \
    gcc \
    git \
    curl \
    wget \
    nano \
    htop

log_success "System dependencies installed"

# Verify Python version
PYTHON_VERSION=$(python3 --version | cut -d ' ' -f 2)
log_success "Python version: $PYTHON_VERSION"

################################################################################
# SECTION 3: VIRTUAL ENVIRONMENT
################################################################################

section_header "STEP 3: PYTHON VIRTUAL ENVIRONMENT"

if [ -d "$HOME/airflow-venv" ]; then
    log_warning "Virtual environment already exists. Removing..."
    rm -rf ~/airflow-venv
fi

log_info "Creating virtual environment..."
python3 -m venv ~/airflow-venv

log_info "Activating virtual environment..."
source ~/airflow-venv/bin/activate

# Make activation permanent in bashrc
if ! grep -q "source ~/airflow-venv/bin/activate" ~/.bashrc; then
    echo 'source ~/airflow-venv/bin/activate' >> ~/.bashrc
fi

log_success "Virtual environment created and activated"

################################################################################
# SECTION 4: AIRFLOW INSTALLATION
################################################################################

section_header "STEP 4: APACHE AIRFLOW INSTALLATION"

log_info "Upgrading pip, setuptools, wheel..."
pip install --upgrade -q pip setuptools wheel

log_info "Installing Apache Airflow 2.9.3 with constraints..."
AIRFLOW_VERSION=2.9.3
PYTHON_VERSION="$(python3 --version | cut -d ' ' -f 2 | cut -d '.' -f 1-2)"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" -q

log_info "Installing Airflow providers..."
pip install \
    apache-airflow-providers-amazon==8.25.0 \
    apache-airflow-providers-databricks==6.7.0 \
    boto3 \
    psycopg2-binary \
    -q

log_success "Apache Airflow $AIRFLOW_VERSION installed"

# Verify installation
INSTALLED_VERSION=$(airflow version)
log_success "Airflow version: $INSTALLED_VERSION"

################################################################################
# SECTION 5: AIRFLOW DIRECTORY & DATABASE INITIALIZATION
################################################################################

section_header "STEP 5: AIRFLOW DIRECTORY & DATABASE"

# Create directory structure
log_info "Creating Airflow directory structure..."
mkdir -p ~/airflow/{dags,logs,plugins,config}

log_success "Directories created"

# Initialize database
log_info "Initializing Airflow database..."
airflow db migrate

log_success "Database initialized"

################################################################################
# SECTION 6: AIRFLOW CONFIGURATION
################################################################################

section_header "STEP 6: AIRFLOW CONFIGURATION"

log_info "Creating optimized airflow.cfg..."

# Backup original if it exists
if [ -f ~/airflow/airflow.cfg ]; then
    cp ~/airflow/airflow.cfg ~/airflow/airflow.cfg.backup
    log_warning "Backed up original config to airflow.cfg.backup"
fi

# Create new optimized config
cat > ~/airflow/airflow.cfg << 'EOF'
[core]
dags_folder = /home/ubuntu/airflow/dags
plugins_folder = /home/ubuntu/airflow/plugins
base_log_folder = /home/ubuntu/airflow/logs
executor = SequentialExecutor
sql_alchemy_conn = sqlite:////home/ubuntu/airflow/airflow.db
load_examples = False
max_active_runs_per_dag = 1
dag_orientation = LR
parallelism = 32
max_active_tasks_per_dag = 16
default_view = graph

[webserver]
web_server_host = 0.0.0.0
web_server_port = 8080
expose_config = False
expose_hostname = False
expose_stacktrace = False
web_server_worker_class = sync
workers = 4
worker_class = sync
worker_timeout = 120
base_url = http://localhost:8080

[scheduler]
dag_dir_list_interval = 30
catchup_by_default = False
dag_file_stat_interval = 10

[logging]
log_format = [%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s
simple_log_format = %(asctime)s - %(levelname)s - %(message)s

[database]
sql_engine_encoding = utf-8

[email]
email_backend = airflow.providers.smtp.utils.smtp_email_backend.SmtpEmailBackend
EOF

log_success "Configuration file created"

# Verify config
if grep -q "executor = SequentialExecutor" ~/airflow/airflow.cfg; then
    log_success "Executor correctly set to SequentialExecutor"
else
    log_error "Executor not set correctly!"
    exit 1
fi

if grep -q "web_server_host = 0.0.0.0" ~/airflow/airflow.cfg; then
    log_success "Web server host correctly set to 0.0.0.0"
else
    log_error "Web server host not set correctly!"
    exit 1
fi

################################################################################
# SECTION 7: AIRFLOW ADMIN USER
################################################################################

section_header "STEP 7: CREATE ADMIN USER"

log_info "Creating admin user..."
airflow users create \
    --username admin \
    --firstname Airflow \
    --lastname Admin \
    --role Admin \
    --email admin@capstone.com \
    --password admin123

log_success "Admin user created"
log_warning "Default credentials: admin / admin123"

################################################################################
# SECTION 8: AIRFLOW CONNECTIONS
################################################################################

section_header "STEP 8: CONFIGURE CONNECTIONS"

log_info "Adding AWS connection..."
airflow connections add 'aws_default' \
    --conn-type 'aws' \
    --conn-extra "{
      \"aws_access_key_id\": \"${AWS_ACCESS_KEY}\",
      \"aws_secret_access_key\": \"${AWS_SECRET_KEY}\",
      \"region_name\": \"us-east-1\"
    }" \
    2>/dev/null || log_warning "AWS connection may already exist"

log_success "AWS connection configured"

log_info "Adding Databricks connection..."
airflow connections add 'databricks_default' \
    --conn-type 'databricks' \
    --conn-host "$DATABRICKS_URL" \
    --conn-extra "{
      \"token\": \"${DATABRICKS_TOKEN}\",
      \"host\": \"${DATABRICKS_URL}\"
    }" \
    2>/dev/null || log_warning "Databricks connection may already exist"

log_success "Databricks connection configured"

# List connections
log_info "Verifying connections..."
airflow connections list --output table | head -10

################################################################################
# SECTION 9: SYSTEMD SERVICES
################################################################################

section_header "STEP 9: SYSTEMD SERVICES (Auto-Start on Reboot)"

log_info "Creating systemd service for Airflow Scheduler..."
sudo tee /etc/systemd/system/airflow-scheduler.service > /dev/null << EOF
[Unit]
Description=Airflow Scheduler
After=network.target

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/airflow
Environment="PATH=/home/ubuntu/airflow-venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"
Environment="AIRFLOW_HOME=/home/ubuntu/airflow"
ExecStart=/home/ubuntu/airflow-venv/bin/airflow scheduler
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

log_success "Scheduler service created"

log_info "Creating systemd service for Airflow Webserver..."
sudo tee /etc/systemd/system/airflow-webserver.service > /dev/null << EOF
[Unit]
Description=Airflow Webserver
After=network.target airflow-scheduler.service

[Service]
Type=simple
User=ubuntu
Group=ubuntu
WorkingDirectory=/home/ubuntu/airflow
Environment="PATH=/home/ubuntu/airflow-venv/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"
Environment="AIRFLOW_HOME=/home/ubuntu/airflow"
ExecStart=/home/ubuntu/airflow-venv/bin/airflow webserver --port 8080
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
EOF

log_success "Webserver service created"

log_info "Reloading systemd daemon..."
sudo systemctl daemon-reload

log_info "Enabling services to start on boot..."
sudo systemctl enable airflow-scheduler.service
sudo systemctl enable airflow-webserver.service

log_success "Services enabled for auto-start on reboot"

################################################################################
# SECTION 10: START SERVICES
################################################################################

section_header "STEP 10: START AIRFLOW SERVICES"

log_info "Starting Airflow Scheduler..."
sudo systemctl start airflow-scheduler.service
sleep 5

log_info "Starting Airflow Webserver..."
sudo systemctl start airflow-webserver.service
sleep 10

log_success "Services started!"

################################################################################
# SECTION 11: VERIFICATION & HEALTH CHECKS
################################################################################

section_header "STEP 11: HEALTH CHECKS"

# Check scheduler
log_info "Checking Scheduler status..."
if sudo systemctl is-active --quiet airflow-scheduler.service; then
    log_success "Scheduler is running"
else
    log_error "Scheduler is NOT running!"
    sudo journalctl -u airflow-scheduler.service -n 20
    exit 1
fi

# Check webserver
log_info "Checking Webserver status..."
if sudo systemctl is-active --quiet airflow-webserver.service; then
    log_success "Webserver is running"
else
    log_error "Webserver is NOT running!"
    sudo journalctl -u airflow-webserver.service -n 20
    exit 1
fi

# Check port
log_info "Checking port 8080..."
if sudo lsof -i :8080 > /dev/null 2>&1; then
    log_success "Port 8080 is listening"
else
    log_error "Port 8080 is NOT listening!"
    exit 1
fi

# Check localhost connectivity
log_info "Testing localhost connectivity..."
RESPONSE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:8080/login)
if [ "$RESPONSE" == "302" ] || [ "$RESPONSE" == "200" ]; then
    log_success "Webserver responding (HTTP $RESPONSE)"
else
    log_warning "Webserver response: HTTP $RESPONSE"
fi

# Check database
log_info "Checking database..."
if [ -f ~/airflow/airflow.db ]; then
    log_success "Database file exists"
else
    log_error "Database file not found!"
    exit 1
fi

################################################################################
# SECTION 12: SUMMARY & NEXT STEPS
################################################################################

section_header "INSTALLATION COMPLETE! ✓"

echo ""
echo "╔════════════════════════════════════════════════════════════════════════╗"
echo "║                   AIRFLOW IS READY TO USE! 🎉                         ║"
echo "╚════════════════════════════════════════════════════════════════════════╝"
echo ""

# Get public IP
PUBLIC_IP=$(curl -s http://checkip.amazonaws.com)

echo -e "${GREEN}📊 AIRFLOW WEB UI${NC}"
echo "  URL: http://${PUBLIC_IP}:8080"
echo "  Username: admin"
echo "  Password: admin123"
echo ""

echo -e "${GREEN}🔧 USEFUL COMMANDS${NC}"
echo "  Check scheduler: sudo systemctl status airflow-scheduler"
echo "  Check webserver: sudo systemctl status airflow-webserver"
echo "  View scheduler logs: sudo journalctl -u airflow-scheduler -f"
echo "  View webserver logs: sudo journalctl -u airflow-webserver -f"
echo "  Stop services: sudo systemctl stop airflow-scheduler airflow-webserver"
echo "  Start services: sudo systemctl start airflow-scheduler airflow-webserver"
echo "  List DAGs: airflow dags list"
echo "  Trigger DAG: airflow dags trigger batch_pipeline --conf '{\"batch_number\":\"2\"}'"
echo ""

echo -e "${GREEN}📁 IMPORTANT PATHS${NC}"
echo "  Dags folder: ~/airflow/dags/"
echo "  Logs folder: ~/airflow/logs/"
echo "  Config file: ~/airflow/airflow.cfg"
echo "  Database: ~/airflow/airflow.db"
echo ""

echo -e "${YELLOW}⚠️  NEXT STEPS:${NC}"
echo "  1. Open browser: http://${PUBLIC_IP}:8080"
echo "  2. Login with admin / admin123"
echo "  3. Go to Admin → Connections to verify:"
echo "     • aws_default (should have green checkmark)"
echo "     • databricks_default (should have green checkmark)"
echo "  4. Deploy your DAG files to ~/airflow/dags/"
echo "  5. Refresh the UI (F5) to see your DAGs"
echo ""

echo -e "${GREEN}💾 CONFIGURATION SUMMARY${NC}"
echo "  S3 Bucket: $BUCKET_NAME"
echo "  Databricks: $DATABRICKS_URL"
echo "  Executor: SequentialExecutor"
echo "  Database: SQLite"
echo ""

echo -e "${BLUE}📝 USEFUL INFORMATION${NC}"
echo "  • Services auto-restart on reboot"
echo "  • Check status: sudo systemctl status airflow-*"
echo "  • View real-time logs: sudo journalctl -u airflow-scheduler -f"
echo "  • Webserver address: 0.0.0.0:8080 (accessible from anywhere)"
echo ""

log_success "Setup completed successfully!"

################################################################################
# SECTION 13: OPTIONAL - SAVE CONFIGURATION
################################################################################

# Save config to file
CONFIG_FILE=~/airflow_config_summary.txt
cat > "$CONFIG_FILE" << EOF
=== AIRFLOW CONFIGURATION SUMMARY ===
Created: $(date)

PUBLIC_IP: $PUBLIC_IP
BUCKET_NAME: $BUCKET_NAME
DATABRICKS_URL: $DATABRICKS_URL
AIRFLOW_HOME: $AIRFLOW_HOME

WEB UI ACCESS:
  URL: http://${PUBLIC_IP}:8080
  Username: admin
  Password: admin123

AWS CONNECTION:
  Type: aws_default
  Access Key: ${AWS_ACCESS_KEY:0:10}...
  Region: us-east-1

DATABRICKS CONNECTION:
  Type: databricks_default
  URL: $DATABRICKS_URL
  Token: ${DATABRICKS_TOKEN:0:10}...

SERVICES:
  Scheduler: sudo systemctl status airflow-scheduler
  Webserver: sudo systemctl status airflow-webserver
  Start all: sudo systemctl start airflow-*
  Stop all: sudo systemctl stop airflow-*

LOGS:
  Scheduler: sudo journalctl -u airflow-scheduler -f
  Webserver: sudo journalctl -u airflow-webserver -f
  DAGs folder: ~/airflow/dags/

TROUBLESHOOTING:
  If webserver won't start, check: sudo journalctl -u airflow-webserver -n 50
  If scheduler issues, check: sudo journalctl -u airflow-scheduler -n 50
  Port conflicts? sudo lsof -i :8080
EOF

log_success "Configuration saved to: $CONFIG_FILE"

echo ""
echo "======================================================================"
echo "  🚀 You're all set! Your Airflow is ready to orchestrate!"
echo "======================================================================"
echo ""
