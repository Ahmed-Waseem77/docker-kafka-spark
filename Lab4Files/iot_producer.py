# iot_producer.py
from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
import threading

class IoTSensorProducer:
    def __init__(self, bootstrap_servers='172.25.0.12:9092'):
        """Initialize Kafka producer with configuration"""
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            # Producer configurations for reliability
            acks='all',  # Wait for all replicas
            retries=3,
            max_in_flight_requests_per_connection=1,
            compression_type='gzip'
        )
        
        self.sensor_types = ['temperature', 'humidity', 'pressure', 'motion']
        self.locations = ['building_a', 'building_b', 'building_c']
        self.running = True
    
    def generate_sensor_data(self, sensor_id):
        """Generate realistic sensor readings"""
        sensor_type = self.sensor_types[sensor_id % len(self.sensor_types)]
        location = self.locations[sensor_id % len(self.locations)]
        
        # Generate realistic values based on sensor type
        if sensor_type == 'temperature':
            value = 20 + random.gauss(0, 5)  # Celsius
            unit = 'celsius'
        elif sensor_type == 'humidity':
            value = 50 + random.gauss(0, 15)  # Percentage
            unit = 'percent'
        elif sensor_type == 'pressure':
            value = 1013 + random.gauss(0, 10)  # hPa
            unit = 'hPa'
        else:  # motion
            value = random.choice([0, 1])  # Binary
            unit = 'boolean'
        
        return {
            'sensor_id': 'sensor_{}'.format(sensor_id),  # Fixed for Python 3.5
            'sensor_type': sensor_type,
            'location': location,
            'value': round(value, 2),
            'unit': unit,
            'timestamp': datetime.now().isoformat(),
            'quality': random.choice(['good', 'good', 'good', 'degraded'])
        }
    
    def produce_sensor_stream(self, sensor_id, interval=1):
        """Continuously produce sensor data"""
        while self.running:
            try:
                data = self.generate_sensor_data(sensor_id)
                
                # Send to Kafka with partition key for ordering
                self.producer.send(
                    topic='iot-sensors',
                    key=data['sensor_id'],
                    value=data,
                    timestamp_ms=int(time.time() * 1000)
                )
                
                # Fixed print statement for Python 3.5
                print("📡 Sent: {sensor_id} - {sensor_type}: {value}".format(
                    sensor_id=data['sensor_id'], 
                    sensor_type=data['sensor_type'], 
                    value=data['value'])
                )
                time.sleep(interval + random.uniform(-0.5, 0.5))
            except Exception as e:
                # Fixed error print statement for Python 3.5
                print("❌ Error producing data: {}".format(e))
                time.sleep(5)
    
    def start_simulation(self, num_sensors=10):
        """Start multi-threaded sensor simulation"""
        # Fixed print statement for Python 3.5
        print("🚀 Starting IoT simulation with {} sensors...".format(num_sensors))
        
        # Create topic if not exists
        from kafka.admin import KafkaAdminClient, NewTopic
        admin = KafkaAdminClient(bootstrap_servers='172.25.0.12:9092')
        try:
            topic = NewTopic(name='iot-sensors', num_partitions=3, replication_factor=1)
            admin.create_topics([topic])
            print("✅ Created topic 'iot-sensors'")
        except:
            print("ℹ️ Topic 'iot-sensors' already exists")
        
        # Start producer threads
        threads = []
        for i in range(num_sensors):
            thread = threading.Thread(
                target=self.produce_sensor_stream,
                args=(i, random.uniform(0.5, 2))
            )
            thread.start()
            threads.append(thread)
        
        try:
            # Run for specified duration
            time.sleep(60)  # Run for 1 minute
        except KeyboardInterrupt:
            # Fixed print statement for Python 3.5
            print("\n⏹️ Stopping simulation...")
        finally:
            self.running = False
            for thread in threads:
                thread.join()
            self.producer.close()
            print("✅ Simulation stopped")

# Run the producer
if __name__ == "__main__":
    producer = IoTSensorProducer()
    producer.start_simulation(num_sensors=5)
