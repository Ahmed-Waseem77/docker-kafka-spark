# Lab 4: Stream Processing with Kafka and Spark Streaming

## Setup Instructions

### Prerequisites

-   Completed Lab 3 (Spark installation)
-   8GB+ RAM (16GB recommended)
-   Ubuntu 20.04+ / macOS / Windows with WSL2

### Installation Steps

1.  **Install Kafka** (if not already installed)

``` bash
cd ~
wget https://downloads.apache.org/kafka/3.3.0/kafka_2.13-3.3.0.tgz
tar -xzf kafka_2.13-3.3.0.tgz
sudo mv kafka_2.13-3.3.0 /opt/kafka

# Add to ~/.bashrc
echo 'export KAFKA_HOME=/opt/kafka' >> ~/.bashrc
echo 'export PATH=$PATH:$KAFKA_HOME/bin' >> ~/.bashrc
source ~/.bashrc
```

2.  **Install Python Dependencies**

``` bash
# Activate virtual environment from Lab 3
source spark_env/bin/activate

# Install packages
pip install kafka-python==2.0.2
pip install confluent-kafka==1.9.2
pip install avro-python3==1.11.1
pip install flask==2.3.2
pip install plotly==5.14.1
```

3.  **Verify Installation**

``` bash
python verify_setup.py
```

## Running the Lab

### Start Kafka Services

Open a terminal and run:

``` bash
# Start Zookeeper
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties

# Wait 5 seconds
sleep 5

# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties

# Verify
jps | grep -E 'Kafka|QuorumPeerMain'
```

### Part 1: Basic Producer/Consumer

**Terminal 1 - Producer:**

``` bash
source spark_env/bin/activate
python iot_producer.py
```

**Terminal 2 - Consumer:**

``` bash
source spark_env/bin/activate
python iot_consumer.py
```

### Part 2: Structured Streaming

**Terminal 3 - Streaming Processor:**

``` bash
source spark_env/bin/activate
python structured_streaming_processor.py
```

Access Spark UI at: http://localhost:4040

### Part 3: Complete Dashboard Application

**Terminal 4 - Dashboard App:**

``` bash
source spark_env/bin/activate
python iot_streaming_app.py
```

Access dashboard at: http://localhost:5000

### Part 4: Performance Testing

``` bash
source spark_env/bin/activate
python performance_test.py
```

## File Structure

    lab4/
    ├── iot_producer.py                    # Kafka producer (Part 1)
    ├── iot_consumer.py                    # Kafka consumer (Part 1)
    ├── structured_streaming_processor.py  # Structured streaming (Part 2)
    ├── iot_streaming_app.py              # Complete application (Part 3)
    ├── dashboard.html                     # Dashboard template
    ├── performance_test.py                # Load testing (Part 4)
    ├── verify_setup.py                    # Setup verification script
    └── README.md                          # This file

## Common Issues

### Kafka Connection Refused

``` bash
# Check if Kafka is running
jps | grep Kafka

# Check logs
tail -f /opt/kafka/logs/server.log

# Restart if needed
$KAFKA_HOME/bin/kafka-server-stop.sh
sleep 3
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties
```

### Port Already in Use

``` bash
# For Spark UI (port 4040)
# Kill existing Spark applications or use different port in code

# For Flask (port 5000)
# Change port in iot_streaming_app.py
```

### Out of Memory

``` bash
# Increase executor memory in Spark configurations
# Edit the .config() calls in the Python scripts
```

## Stopping Everything

``` bash
# Stop all Python scripts (Ctrl+C in each terminal)

# Stop Kafka
$KAFKA_HOME/bin/kafka-server-stop.sh
$KAFKA_HOME/bin/zookeeper-server-stop.sh

# Verify stopped
jps
```

## Deliverables Checklist

-   [ ] All Python scripts completed and tested
-   [ ] Architecture diagram created
-   [ ] Performance metrics collected
-   [ ] Screenshots captured (Kafka, Spark UI, Dashboard)
-   [ ] Analysis document completed (2 pages)
-   [ ] Reflection questions answered
-   [ ] All files zipped for submission

## Submission

Create submission package:

``` bash
zip -r lab4_submission.zip \
    iot_producer.py \
    iot_consumer.py \
    structured_streaming_processor.py \
    iot_streaming_app.py \
    performance_test.py \
    dashboard.html \
    streaming_report.pdf \
    screenshots/ \
    analysis.md \
    README.md
```

Submit via Canvas by the deadline.

## Resources

-   [Kafka Documentation](https://kafka.apache.org/documentation/)
-   [Spark Streaming
    Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
-   [Structured Streaming
    Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

## Getting Help

-   Check troubleshooting section above
-   Review Spark UI for errors
-   Check Kafka logs
-   Ask on course forum with error messages
