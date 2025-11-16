#!/usr/bin/env python3
"""
Lab 4 Part 4: Performance Testing
Load testing for the IoT streaming pipeline
"""

import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError
import threading
from collections import defaultdict
import sys

class PerformanceTester:
    """Performance testing for IoT streaming pipeline"""
    
    def __init__(self):
        self.producer = None
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'total_latency': 0.0,
            'start_time': None,
            'end_time': None,
            'errors': []
        }
        self.lock = threading.Lock()
        
    def connect_kafka(self):
        """Connect to Kafka broker"""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=['172.25.0.12:9092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=5,
                compression_type='gzip'
            )
            print("✓ Connected to Kafka broker")
            return True
        except Exception as e:
            print(f"❌ Failed to connect to Kafka: {e}")
            return False
    
    def generate_sensor_data(self):
        """Generate realistic sensor data"""
        sensor_types = ['temperature', 'humidity', 'pressure', 'light', 'motion']
        locations = ['Building-A', 'Building-B', 'Building-C', 'Warehouse', 'Lab']
        
        sensor_id = f"SENSOR-{random.randint(1, 100):03d}"
        sensor_type = random.choice(sensor_types)
        
        # Generate realistic values
        base_temp = 22.0
        temp_variation = random.uniform(-10, 15)
        
        return {
            'sensor_id': sensor_id,
            'sensor_type': sensor_type,
            'temperature': round(base_temp + temp_variation, 2),
            'humidity': round(random.uniform(30, 80), 2),
            'timestamp': datetime.now().isoformat(),
            'location': random.choice(locations),
            'battery_level': round(random.uniform(10, 100), 2),
            'signal_strength': round(random.uniform(-90, -30), 2)
        }
    
    def send_message(self):
        """Send a single message and track performance"""
        try:
            data = self.generate_sensor_data()
            start_time = time.time()
            
            # The .send() method is asynchronous and returns a Future.
            # .get() blocks until the producer receives a response from the broker.
            future = self.producer.send('iot-sensors', value=data)
            record_metadata = future.get(timeout=10)
            
            latency = (time.time() - start_time) * 1000  # Convert to ms
            
            with self.lock:
                self.stats['messages_sent'] += 1
                self.stats['total_latency'] += latency
                
            return True
            
        except KafkaError as e:
            with self.lock:
                self.stats['messages_failed'] += 1
                # Record the error only if it's not already in the sample
                if len(self.stats['errors']) < 3:
                    self.stats['errors'].append(f"KafkaError: {type(e).__name__} - {e}")
            return False
        except Exception as e:
            with self.lock:
                self.stats['messages_failed'] += 1
                if len(self.stats['errors']) < 3:
                    self.stats['errors'].append(f"GeneralError: {type(e).__name__} - {e}")
            return False
    
    def worker_thread(self, messages_per_thread, thread_id):
        """Worker thread to send messages (Not used in sustained test but kept for burst tests)"""
        print(f"  Thread {thread_id}: Starting to send {messages_per_thread} messages")
        
        for i in range(messages_per_thread):
            self.send_message()
            
            # Small delay to avoid overwhelming the system
            if i % 100 == 0:
                time.sleep(0.01)
        
        print(f"  Thread {thread_id}: Completed")
    
    def run_test(self, total_messages=1000, num_threads=4):
        """Run performance test (Burst/Volume Test)"""
        
        print("\n" + "="*70)
        print("IoT Streaming Pipeline - Burst/Volume Test")
        print("="*70)
        print(f"\nTest Configuration:")
        print(f"  Total Messages: {total_messages:,}")
        print(f"  Concurrent Threads: {num_threads}")
        print(f"  Messages per Thread: {total_messages // num_threads:,}")
        print(f"  Target Topic: iot-sensors")
        print("\n" + "="*70)
        
        # Connect to Kafka
        if not self.connect_kafka():
            return
        
        # Calculate messages per thread
        messages_per_thread = total_messages // num_threads
        
        # Start test
        self.stats['start_time'] = time.time()
        print(f"\n🚀 Starting test at {datetime.now().strftime('%H:%M:%S')}\n")
        
        # Create and start threads
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(
                target=self.worker_thread,
                args=(messages_per_thread, i + 1)
            )
            thread.start()
            threads.append(thread)
        
        # Wait for all threads to complete
        for thread in threads:
            thread.join()
        
        self.stats['end_time'] = time.time()
        
        # Close producer
        self.producer.flush()
        self.producer.close()
        
        # Print results
        self.print_results()
    
    def print_results(self):
        """Print test results"""
        duration = self.stats['end_time'] - self.stats['start_time']
        
        # Handle division by zero for sent messages or duration
        messages_sent = self.stats['messages_sent']
        messages_total = messages_sent + self.stats['messages_failed']
        
        avg_latency = (self.stats['total_latency'] / messages_sent
                       if messages_sent > 0 else 0)
        throughput = messages_sent / duration if duration > 0 else 0
        success_rate = (messages_sent / messages_total * 100
                        if messages_total > 0 else 0)
        
        print("\n" + "="*70)
        print("Performance Test Results")
        print("="*70)
        print(f"\n📊 Message Statistics:")
        print(f"  Total Messages Attempted:  {messages_total:,}")
        print(f"  Successful Messages Sent:  {messages_sent:,}")
        print(f"  Failed Messages:           {self.stats['messages_failed']:,}")
        print(f"  Success Rate:              {success_rate:.2f}%")
        
        print(f"\n⏱️  Timing Metrics:")
        print(f"  Total Duration:            {duration:.2f} seconds")
        print(f"  Average Latency:           {avg_latency:.2f} ms")
        print(f"  Throughput:                {throughput:.2f} messages/second")
        
        print(f"\n💻 System Performance:")
        print(f"  Messages per Second:       {throughput:.0f} msg/s")
        print(f"  Messages per Minute:       {throughput * 60:.0f} msg/min")
        
        if self.stats['errors']:
            print(f"\n⚠️  Errors Encountered: {len(self.stats['errors'])} distinct error types (Sample of 3)")
            print("  Sample errors:")
            for error in self.stats['errors']:
                print(f"    - {error}")
        
        print("\n" + "="*70)
        print("Test completed successfully! ✓")
        print("="*70 + "\n")
    
    def run_sustained_load_test(self, duration_seconds=60, rate_per_second=100):
        """Run sustained load test"""
        
        print("\n" + "="*70)
        print("Sustained Load Test")
        print("="*70)
        print(f"\nTest Configuration:")
        print(f"  Duration: {duration_seconds} seconds")
        print(f"  Target Rate: {rate_per_second:,} messages/second")
        print(f"  Expected Total: ~{duration_seconds * rate_per_second:,} messages")
        print("\n" + "="*70)
        
        if not self.connect_kafka():
            return
        
        # Reset stats before a new run
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'total_latency': 0.0,
            'start_time': None,
            'end_time': None,
            'errors': []
        }

        self.stats['start_time'] = time.time()
        # Time interval between messages to maintain rate
        interval = 1.0 / rate_per_second
        end_time = time.time() + duration_seconds
        
        print(f"\n🚀 Starting sustained load at {datetime.now().strftime('%H:%M:%S')}")
        print(f"Sending {rate_per_second:,} messages/second for {duration_seconds} seconds...\n")
        
        message_count = 0
        last_report = time.time()
        
        while time.time() < end_time:
            time_before_send = time.time()
            self.send_message()
            message_count += 1
            
            # Calculate time spent and time to sleep
            time_spent = time.time() - time_before_send
            sleep_duration = interval - time_spent
            
            if sleep_duration > 0:
                time.sleep(sleep_duration)
            
            # Report progress every 10 seconds
            if time.time() - last_report >= 10 and (time.time() < end_time or message_count == 1):
                elapsed = time.time() - self.stats['start_time']
                current_sent = self.stats['messages_sent'] + self.stats['messages_failed']
                current_rate = current_sent / elapsed
                print(f"  Progress: {elapsed:.0f}s elapsed | "
                      f"{current_sent:,} total attempts | "
                      f"Rate: {current_rate:.1f} msg/s")
                last_report = time.time()
        
        self.stats['end_time'] = time.time()
        
        self.producer.flush()
        self.producer.close()
        
        self.print_results()

def print_menu():
    """Print test menu"""
    print("\n" + "="*70)
    print("IoT Streaming Performance Testing Suite")
    print("="*70)
    print("\nSustained Load Tests:")
    print("  1. Baseline: 500 messages/sec over 30 sec (~15,000 total)")
    print("  2. Moderate: 1,000 messages/sec over 60 sec (~60,000 total)")
    print("  3. Stress:   5,000 messages/sec over 120 sec (~600,000 total)")
    print("\nUtility:")
    print("  4. Custom Sustained Load Test")
    print("  0. Exit")
    print("="*70)

def run_custom_test():
    """Run custom sustained load test with user inputs"""
    try:
        print("\nCustom Sustained Load Test Configuration:")
        rate = int(input("  Target messages per second (e.g., 500): "))
        duration = int(input("  Duration in seconds (e.g., 60): "))
        
        if rate <= 0 or duration <= 0:
            print("❌ Rate and duration must be positive integers.")
            return

        tester = PerformanceTester()
        tester.run_sustained_load_test(duration_seconds=duration, rate_per_second=rate)
    except ValueError:
        print("❌ Invalid input. Please enter numbers only.")
    except KeyboardInterrupt:
        print("\n\nTest cancelled by user.")

def main():
    """Main function"""
    
    while True:
        print_menu()
        
        try:
            choice = input("\nEnter your choice (0-4): ").strip()
            
            if choice == '0':
                print("\nExiting performance tester. Goodbye!")
                break
            elif choice == '1':
                # Baseline: 500 messages/sec sustained over 30 sec
                tester = PerformanceTester()
                tester.run_sustained_load_test(duration_seconds=30, rate_per_second=500)
            elif choice == '2':
                # Moderate: 1000 messages/sec sustained over 60 sec
                tester = PerformanceTester()
                tester.run_sustained_load_test(duration_seconds=60, rate_per_second=1000)
            elif choice == '3':
                # Stress: 5000 messages/sec sustained over 120 sec
                tester = PerformanceTester()
                tester.run_sustained_load_test(duration_seconds=120, rate_per_second=5000)
            elif choice == '4':
                run_custom_test()
            else:
                print("❌ Invalid choice. Please enter a number between 0 and 4.")
            
            input("\nPress Enter to continue...")
            
        except KeyboardInterrupt:
            print("\n\nExiting performance tester. Goodbye!")
            break
        except Exception as e:
            print(f"\n❌ An unexpected error occurred: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    main()
