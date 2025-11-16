from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError
import json
import time
from collections import defaultdict

class IoTConsumer:
    def __init__(self, topics, group_id='iot-analytics'):
        """Initialize consumer with configuration"""
        self.consumer = KafkaConsumer(
            *topics,
            bootstrap_servers=['172.25.0.13:9092'],
            group_id=group_id,
            value_deserializer=self._safe_deserialize,
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',
            enable_auto_commit=False,  # Manual commit for exactly-once
            max_poll_records=100,
            session_timeout_ms=30000,
            heartbeat_interval_ms=10000
        )
        
        self.metrics = defaultdict(list)
        self.alert_thresholds = {
            'temperature': (15, 30),
            'humidity': (30, 70),
            'pressure': (1000, 1030)
        }
    
    def _safe_deserialize(self, message_bytes):
        """Safe JSON deserializer with error handling"""
        if message_bytes is None:
             return None
        try:
             return json.loads(message_bytes.decode('utf-8'))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
             # Fixed f-string for Python 3.5
             print("⚠️ Deserialization error: {}".format(e))
             return None

    def process_message(self, message):
        """Process individual sensor reading"""
        data = message.value
        
        if not data or 'sensor_type' not in data:
            # Handle messages that failed deserialization
            return False 
            
        sensor_type = data['sensor_type']
        value = data['value']
        
        # Store metrics
        self.metrics[sensor_type].append(value)
        
        # Check for alerts
        if sensor_type in self.alert_thresholds:
            min_val, max_val = self.alert_thresholds[sensor_type]
            if value < min_val or value > max_val:
                self.trigger_alert(data, min_val, max_val)
        
        # Calculate statistics every 10 readings
        if len(self.metrics[sensor_type]) >= 10:
            self.calculate_statistics(sensor_type)
            # Keep only the last 10 readings for the next calculation batch
            self.metrics[sensor_type] = self.metrics[sensor_type][-10:] 
        
        return True
    
    def trigger_alert(self, data, min_val, max_val):
        """Handle threshold violations"""
        # Fixed f-string for Python 3.5
        alert_message = "🚨 ALERT: {} - {} = {} (threshold: {}-{})".format(
            data['sensor_id'], 
            data['sensor_type'], 
            data['value'], 
            min_val, 
            max_val
        )
        print(alert_message)
        # Here you could send notifications, write to database, etc.
    
    def calculate_statistics(self, sensor_type):
        """Calculate running statistics"""
        values = self.metrics[sensor_type]
        if values:
            avg = sum(values) / len(values)
            min_val = min(values)
            max_val = max(values)
            
            # Fixed f-string for Python 3.5, including float formatting
            stats_message = "📊 Stats for {}: Avg={:.2f}, Min={:.2f}, Max={:.2f}".format(
                sensor_type, avg, min_val, max_val
            )
            print(stats_message)
    
    def consume_messages(self):
        """Main consumption loop with error handling"""
        print("🎧 Starting IoT consumer...")
        
        try:
            for message in self.consumer:
                success = False
                try:
                    # Process the message
                    success = self.process_message(message)
                    
                    if success:
                        # Commit the offset only if processing was successful
                        self.consumer.commit()
                        
                except Exception as e:
                    # Fixed f-string for Python 3.5
                    print("❌ Error processing message: {}".format(e))
                    # Note: We do NOT commit here, allowing us to re-read the message later
                    continue 
                
                # Print consumer lag periodically
                self.print_consumer_lag()
                
        except KeyboardInterrupt:
            print("\n⏹️ Stopping consumer...") # Already Python 3.5 compatible
        finally:
            self.consumer.close()
            print("✅ Consumer stopped")

    def print_consumer_lag(self):
        """Monitor consumer lag"""
        for partition in self.consumer.assignment():
            # Get the last committed offset and the current position (offset)
            committed = self.consumer.committed(partition)
            position = self.consumer.position(partition)
            
            if committed is not None:
                lag = position - committed
                if lag > 100:
                    # Fixed f-string for Python 3.5
                    print("⚠️ High lag on {}: {} messages".format(partition, lag))

# Run the consumer
if __name__ == "__main__":
    consumer = IoTConsumer(['iot-sensors'])
    consumer.consume_messages()
