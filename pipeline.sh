#!/bin/bash
# Helper script for pipeline operations

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

echo_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

echo_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if Docker is running
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        echo_error "Docker is not running. Please start Docker Desktop."
        exit 1
    fi
    echo_info "Docker is running ✓"
}

# Function to check if kaggle.json exists
check_kaggle_credentials() {
    if [ ! -f "kaggle.json" ]; then
        echo_error "kaggle.json not found!"
        echo "Please create kaggle.json with your Kaggle API credentials."
        echo "See README.md for instructions."
        exit 1
    fi
    echo_info "Kaggle credentials found ✓"
}

# Function to start the pipeline
start_pipeline() {
    echo_info "Starting data pipeline..."
    check_docker
    check_kaggle_credentials
    
    docker-compose up --build -d
    
    echo ""
    echo_info "Pipeline containers are starting..."
    echo ""
    echo "The pipeline will run automatically and complete in ~3 minutes."
    echo ""
    echo "To view live logs, run: ./pipeline.sh logs"
    echo "To check status, run: ./pipeline.sh status"
    echo ""
}

# Function to stop the pipeline
stop_pipeline() {
    echo_info "Stopping pipeline..."
    docker-compose down
    echo_info "Pipeline stopped ✓"
}

# Function to reset everything
reset_pipeline() {
    echo_warn "This will delete all data and restart from scratch."
    read -p "Are you sure? (y/N) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo_info "Resetting pipeline..."
        docker-compose down -v
        rm -rf data/ logs/
        echo_info "Reset complete ✓"
        echo "Run './pipeline.sh start' to begin fresh."
    else
        echo "Cancelled."
    fi
}

# Function to view logs
view_logs() {
    if [ -z "$1" ]; then
        docker-compose logs -f
    else
        docker-compose logs -f "$1"
    fi
}

# Function to check status
check_status() {
    echo_info "Pipeline Status:"
    echo ""
    docker-compose ps
    echo ""
    echo "Recent pipeline runs:"
    docker-compose exec -T pipeline tail -20 /opt/pipeline/logs/pipeline.log 2>/dev/null || echo "No logs yet - pipeline may not have run."
}

# Function to connect to database
connect_db() {
    echo_info "Connecting to PostgreSQL..."
    docker-compose exec postgres psql -U airflow -d airflow
}

# Function to validate data
validate_data() {
    echo_info "Validating pipeline data..."
    echo ""
    
    echo "=== Bronze Layer ==="
    if [ -f "data/bronze/telco_customer_churn.csv" ]; then
        echo_info "✓ Raw data file exists"
        wc -l data/bronze/telco_customer_churn.csv
    else
        echo_warn "✗ Raw data file not found - pipeline may not have run yet"
    fi
    
    echo ""
    echo "=== Silver Layer ==="
    docker-compose exec -T postgres psql -U airflow -d airflow -c "\
        SELECT COUNT(*) as total_records, \
               SUM(CASE WHEN churned THEN 1 ELSE 0 END) as churned_count \
        FROM silver.customers_staging;" 2>/dev/null || echo_warn "Silver table not ready yet"
    
    echo ""
    echo "=== Gold Layer ==="
    docker-compose exec -T postgres psql -U airflow -d airflow -c "\
        SELECT * FROM gold.executive_summary;" 2>/dev/null || echo_warn "Gold models not ready yet"
}

# Function to manually run pipeline
run_pipeline() {
    echo_info "Manually triggering pipeline..."
    docker-compose exec pipeline python3 /opt/pipeline/orchestrator.py
    echo_info "Pipeline execution complete ✓"
}

# Function to show help
show_help() {
    cat << EOF
Data Pipeline Helper Script (Cron-based Orchestration)

Usage: ./pipeline.sh [command]

Commands:
    start       Start the pipeline (builds containers, runs once)
    stop        Stop the pipeline (keeps data)
    restart     Restart the pipeline
    reset       Stop and delete all data (fresh start)
    run         Manually trigger pipeline execution
    logs        View all logs (or specify service: logs pipeline)
    status      Show container status and recent runs
    db          Connect to PostgreSQL database
    validate    Validate data at each layer
    help        Show this help message

Examples:
    ./pipeline.sh start              # Start everything and run pipeline
    ./pipeline.sh run                # Manually run pipeline again
    ./pipeline.sh logs               # View all logs
    ./pipeline.sh logs pipeline      # View pipeline logs only
    ./pipeline.sh validate           # Check data quality
    ./pipeline.sh db                 # Connect to database
    ./pipeline.sh reset              # Start fresh

Scheduling:
    The pipeline runs automatically on container start.
    To set up recurring runs, edit 'crontab' and rebuild.

For more information, see README.md
EOF
}

# Main script logic
case "${1:-help}" in
    start)
        start_pipeline
        ;;
    stop)
        stop_pipeline
        ;;
    restart)
        stop_pipeline
        start_pipeline
        ;;
    reset)
        reset_pipeline
        ;;
    run)
        run_pipeline
        ;;
    logs)
        view_logs "${2:-}"
        ;;
    status)
        check_status
        ;;
    db)
        connect_db
        ;;
    validate)
        validate_data
        ;;
    help|*)
        show_help
        ;;
esac
