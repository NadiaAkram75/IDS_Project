@echo off
title IDS Project - All-in-One Launcher
echo ====================================================
echo   🛡️  NETWORK INTRUSION DETECTION SYSTEM LAUNCHER
echo ====================================================

echo [0/7] Creating required directories...
if not exist "minio_data" mkdir minio_data
if not exist "data" mkdir data
echo Directories ready!

echo [1/7] Installing Python dependencies...
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
echo Dependencies installed!

echo [2/7] Starting Docker Containers...
docker-compose up -d

echo [3/7] Waiting for MinIO to start...
timeout /t 5 /nobreak > nul

echo [4/7] Ensuring 'ids-bucket' exists in MinIO...
:: This command uses the MinIO client inside the container to create the bucket
docker exec minio mc alias set myminio http://localhost:9000 minioadmin minioadmin
docker exec minio mc mb myminio/ids-bucket
echo Bucket ready!

echo [5/7] Initializing Kafka Topic...
start /min cmd /c "cd producer && python producer.py"
timeout /t 5 /nobreak > nul

echo [6/7] Building Spark Scala Application...
docker run --rm -v "%cd%\spark-app:/app" -w /app hseeberger/scala-sbt:11.0.14.1_1.6.2_2.12.15 sbt package

echo [7/7] Launching Dashboard and Spark Engine...
start cmd /k "streamlit run dashboard/app.py"

docker exec -u 0 -it spark /opt/spark/bin/spark-submit --class IDSPipeline --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 /app/target/scala-2.12/idsstreaming_2.12-1.0.jar

pause