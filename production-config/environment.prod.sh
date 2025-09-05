#!/bin/bash

# Production Environment Variables for DLM Migration
# Source this file before running migrations: source production-config/environment.prod.sh

# Base DLM Environment
export DLM_HOME="/app/tetra/DLM"
export DLM_CONFIG_DIR="$DLM_HOME/config"
export DLM_JAR_DIR="$DLM_HOME/jar"
export DLM_LOG_DIR="$DLM_HOME/logs"
export DLM_SCRIPT_DIR="$DLM_HOME/scripts"
export DLM_TMP_DIR="$DLM_HOME/tmp"

# Java Configuration for Production
export JAVA_HOME="${JAVA_HOME:-/opt/java}"
export JAVA_OPTS="-Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:G1HeapRegionSize=32m"
export JAVA_OPTS="$JAVA_OPTS -XX:+UnlockExperimentalVMOptions -XX:+UseJVMCICompiler"
export JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote"
export JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.port=9999"
export JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.authenticate=false"
export JAVA_OPTS="$JAVA_OPTS -Dcom.sun.management.jmxremote.ssl=false"

# Scala Environment
export SCALA_HOME="${SCALA_HOME:-/opt/scala}"
export PATH="$SCALA_HOME/bin:$PATH"

# SBT Configuration
export SBT_HOME="${SBT_HOME:-/opt/sbt}"
export SBT_OPTS="-Xmx4G -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled"
export PATH="$SBT_HOME/bin:$PATH"

# Spark Configuration
export SPARK_HOME="${SPARK_HOME:-/opt/spark}"
export SPARK_CONF_DIR="$SPARK_HOME/conf"
export SPARK_LOCAL_DIRS="/tmp/spark-local,$DLM_TMP_DIR/spark-local"
export SPARK_WORKER_DIR="/tmp/spark-worker"

# Spark Performance Tuning
export SPARK_DRIVER_MEMORY="8g"
export SPARK_DRIVER_CORES="4"
export SPARK_DRIVER_MAX_RESULT_SIZE="4g"
export SPARK_EXECUTOR_MEMORY="12g"
export SPARK_EXECUTOR_CORES="6"
export SPARK_EXECUTOR_INSTANCES="16"

# Spark Additional Options
export SPARK_DRIVER_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200"
export SPARK_EXECUTOR_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# Hadoop Configuration
export HADOOP_HOME="${HADOOP_HOME:-/opt/hadoop}"
export HADOOP_CONF_DIR="$HADOOP_HOME/etc/hadoop"
export PATH="$HADOOP_HOME/bin:$PATH"

# YARN Configuration
export YARN_CONF_DIR="$HADOOP_CONF_DIR"
export HADOOP_YARN_HOME="$HADOOP_HOME"

# Migration Specific Settings
export MIGRATION_ENVIRONMENT="PROD"
export MIGRATION_LOG_LEVEL="INFO"
export MIGRATION_DATE=$(date +%d%m%Y)
export MIGRATION_TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Database Connection Timeouts (in milliseconds)
export CASSANDRA_CONNECTION_TIMEOUT="30000"
export CASSANDRA_READ_TIMEOUT="120000"
export ORACLE_CONNECTION_TIMEOUT="30000"
export ORACLE_SOCKET_TIMEOUT="60000"

# Performance and Monitoring
export ENABLE_SPARK_METRICS="true"
export ENABLE_JMX_MONITORING="true"
export JMX_PORT="9999"
export METRICS_ENABLED="true"

# Security Settings
export ENABLE_SSL="true"
export SSL_KEYSTORE_PATH="$DLM_HOME/certs"
export SSL_TRUSTSTORE_PATH="$DLM_HOME/certs"
export ENABLE_KERBEROS="${ENABLE_KERBEROS:-false}"

# Logging Configuration
export LOG_ROOT_LEVEL="INFO"
export LOG_SPARK_LEVEL="WARN"
export LOG_HADOOP_LEVEL="WARN"
export LOG_CASSANDRA_LEVEL="WARN"

# Resource Limits
export MAX_EXECUTORS="32"
export MIN_EXECUTORS="4"
export INITIAL_EXECUTORS="8"
export DYNAMIC_ALLOCATION="true"

# Batch Processing Settings
export DEFAULT_BATCH_SIZE="10000"
export MAX_BATCH_SIZE="50000"
export PARALLEL_JOBS="4"
export RETRY_ATTEMPTS="3"
export RETRY_DELAY="5000"

# Checkpoint and Recovery
export CHECKPOINT_DIR="$DLM_HOME/checkpoints"
export STREAMING_CHECKPOINT_DIR="$DLM_HOME/streaming-checkpoints"
export RECOVERY_MODE="CHECKPOINTING"

# Temporary Directories
export SPARK_SQL_WAREHOUSE_DIR="$DLM_HOME/spark-warehouse"
export SPARK_EVENT_LOG_DIR="$DLM_HOME/spark-events"

# Network Configuration
export SPARK_DRIVER_HOST=$(hostname -f)
export SPARK_DRIVER_PORT="7001"
export SPARK_BLOCKMANAGER_PORT="7002"
export SPARK_UI_PORT="4040"
export SPARK_HISTORY_UI_PORT="18080"

# Memory Management
export SPARK_DAEMON_MEMORY="2g"
export SPARK_HISTORY_OPTS="-Xmx2g"

# Classpath Extensions
export SPARK_CLASSPATH="$SPARK_CLASSPATH:$DLM_HOME/lib/*"
export SPARK_CLASSPATH="$SPARK_CLASSPATH:/opt/drivers/*"

# Python Environment (if using PySpark)
export PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.5-src.zip"
export PYSPARK_PYTHON="${PYSPARK_PYTHON:-python3}"

# Application-specific Environment Variables
export APP_NAME="DLM-Migration-Production"
export APP_VERSION="1.0"
export DEPLOY_MODE="cluster"
export SPARK_MASTER="yarn"

# Email Configuration for Alerts (if applicable)
export ALERT_EMAIL_ENABLED="true"
export ALERT_EMAIL_RECIPIENTS="dlm-team@company.com"
export ALERT_EMAIL_SMTP_SERVER="smtp.company.com"

# Backup and Archive Settings
export BACKUP_ENABLED="true"
export BACKUP_DIR="$DLM_HOME/backups"
export ARCHIVE_LOGS_AFTER_DAYS="30"
export CLEANUP_TEMP_FILES="true"

# Timezone Configuration
export TZ="${TZ:-America/New_York}"

# Debug Settings (disable in production)
export SPARK_PRINT_LAUNCH_COMMAND="false"
export SPARK_DEBUG="false"

# Add paths to system PATH
export PATH="$SPARK_HOME/bin:$PATH"
export PATH="$DLM_SCRIPT_DIR:$PATH"

# Create required directories if they don't exist
mkdir -p "$CHECKPOINT_DIR"
mkdir -p "$STREAMING_CHECKPOINT_DIR"
mkdir -p "$SPARK_SQL_WAREHOUSE_DIR"
mkdir -p "$SPARK_EVENT_LOG_DIR"
mkdir -p "$DLM_TMP_DIR/spark-local"
mkdir -p "$BACKUP_DIR"
mkdir -p "$DLM_LOG_DIR/metrics"
mkdir -p "$DLM_LOG_DIR/app-metrics"

echo "DLM Production Environment loaded successfully"
echo "Environment: $MIGRATION_ENVIRONMENT"
echo "Spark Home: $SPARK_HOME"
echo "DLM Home: $DLM_HOME"
echo "Log Level: $MIGRATION_LOG_LEVEL"
echo "Timestamp: $MIGRATION_TIMESTAMP"