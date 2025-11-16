#!/usr/bin/env python3
"""
Lab 4 Part 3: Complete IoT Streaming Application with Dashboard
Real-time dashboard for IoT sensor monitoring using Spark + Flask
"""

from flask import Flask, render_template_string, jsonify
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count, current_timestamp,
    expr, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, TimestampType
)
from threading import Thread
import time
import json

# Flask app
app = Flask(__name__)

# Global variables for dashboard data
dashboard_data = {
    'total_messages': 0,
    'total_alerts': 0,
    'active_sensors': 0,
    'system_health': 'Good',
    'recent_alerts': [],
    'sensor_stats': []
}

# Dashboard HTML template
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>IoT Sensor Monitoring Dashboard</title>
    <style>
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }
        
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #333;
        }
        
        .container {
            max-width: 1400px;
            margin: 0 auto;
        }
        
        h1 {
            color: white;
            text-align: center;
            margin-bottom: 30px;
            font-size: 2.5em;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }
        
        .metrics-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .metric-card {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            transition: transform 0.3s ease;
        }
        
        .metric-card:hover {
            transform: translateY(-5px);
        }
        
        .metric-value {
            font-size: 3em;
            font-weight: bold;
            color: #667eea;
            margin: 10px 0;
        }
        
        .metric-label {
            font-size: 1.1em;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 1px;
        }
        
        .alerts-section {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
            margin-bottom: 30px;
        }
        
        .section-title {
            font-size: 1.8em;
            color: #667eea;
            margin-bottom: 20px;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
        }
        
        .alert-item {
            padding: 15px;
            margin: 10px 0;
            border-left: 5px solid #ff6b6b;
            background: #fff5f5;
            border-radius: 5px;
            animation: slideIn 0.5s ease;
        }
        
        @keyframes slideIn {
            from {
                opacity: 0;
                transform: translateX(-20px);
            }
            to {
                opacity: 1;
                transform: translateX(0);
            }
        }
        
        .alert-type {
            font-weight: bold;
            color: #ff6b6b;
            text-transform: uppercase;
        }
        
        .alert-details {
            color: #666;
            margin-top: 5px;
        }
        
        .sensor-stats-table {
            width: 100%;
            background: white;
            border-radius: 15px;
            overflow: hidden;
            box-shadow: 0 10px 30px rgba(0,0,0,0.2);
        }
        
        table {
            width: 100%;
            border-collapse: collapse;
        }
        
        th {
            background: #667eea;
            color: white;
            padding: 15px;
            text-align: left;
            font-weight: 600;
        }
        
        td {
            padding: 12px 15px;
            border-bottom: 1px solid #eee;
        }
        
        tr:hover {
            background: #f8f9ff;
        }
        
        .status-badge {
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
        }
        
        .status-good {
            background: #d4edda;
            color: #155724;
        }
        
        .status-warning {
            background: #fff3cd;
            color: #856404;
        }
        
        .status-critical {
            background: #f8d7da;
            color: #721c24;
        }
        
        .refresh-info {
            text-align: center;
            color: white;
            margin-top: 20px;
            font-size: 0.9em;
        }
        
        .timestamp {
            color: white;
            text-align: center;
            margin-top: 10px;
            font-size: 1.1em;
        }
    </style>
    <script>
        function refreshData() {
            fetch('/api/metrics')
                .then(response => response.json())
                .then(data => {
                    // Update metrics
                    document.getElementById('total-messages').textContent = data.total_messages;
                    document.getElementById('total-alerts').textContent = data.total_alerts;
                    document.getElementById('active-sensors').textContent = data.active_sensors;
                    document.getElementById('system-health').textContent = data.system_health;
                    
                    // Update health status color
                    const healthElement = document.getElementById('health-badge');
                    healthElement.className = 'status-badge status-' + 
                        (data.system_health === 'Good' ? 'good' : 
                         data.system_health === 'Warning' ? 'warning' : 'critical');
                    
                    // Update alerts
                    const alertsContainer = document.getElementById('alerts-container');
                    alertsContainer.innerHTML = '';
                    data.recent_alerts.slice(0, 5).forEach(alert => {
                        const alertDiv = document.createElement('div');
                        alertDiv.className = 'alert-item';
                        alertDiv.innerHTML = `
                            <div class="alert-type">${alert.type}</div>
                            <div class="alert-details">
                                Sensor: ${alert.sensor_id} | 
                                Location: ${alert.location} | 
                                Time: ${alert.timestamp}
                            </div>
                        `;
                        alertsContainer.appendChild(alertDiv);
                    });
                    
                    // Update sensor statistics
                    const statsBody = document.getElementById('stats-body');
                    statsBody.innerHTML = '';
                    data.sensor_stats.forEach(stat => {
                        const row = document.createElement('tr');
                        row.innerHTML = `
                            <td>${stat.sensor_type}</td>
                            <td>${stat.count}</td>
                            <td>${stat.avg_temp}</td>
                            <td>${stat.avg_humidity}</td>
                            <td>${stat.avg_battery}</td>
                        `;
                        statsBody.appendChild(row);
                    });
                    
                    // Update timestamp
                    document.getElementById('last-update').textContent = 
                        'Last updated: ' + new Date().toLocaleTimeString();
                });
        }
        
        // Refresh every 5 seconds
        setInterval(refreshData, 5000);
        
        // Initial load
        window.onload = refreshData;
    </script>
</head>
<body>
    <div class="container">
        <h1>🌡️ IoT Sensor Monitoring Dashboard</h1>
        
        <div class="metrics-grid">
            <div class="metric-card">
                <div class="metric-label">Total Messages</div>
                <div class="metric-value" id="total-messages">0</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-label">Active Alerts</div>
                <div class="metric-value" id="total-alerts">0</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-label">Active Sensors</div>
                <div class="metric-value" id="active-sensors">0</div>
            </div>
            
            <div class="metric-card">
                <div class="metric-label">System Health</div>
                <div class="metric-value">
                    <span id="health-badge" class="status-badge status-good">
                        <span id="system-health">Good</span>
                    </span>
                </div>
            </div>
        </div>
        
        <div class="alerts-section">
            <h2 class="section-title">🚨 Recent Alerts</h2>
            <div id="alerts-container">
                <p style="color: #666; text-align: center; padding: 20px;">
                    No alerts at this time
                </p>
            </div>
        </div>
        
        <div class="sensor-stats-table">
            <h2 class="section-title" style="padding: 20px 25px 0 25px;">
                📊 Sensor Statistics
            </h2>
            <table>
                <thead>
                    <tr>
                        <th>Sensor Type</th>
                        <th>Message Count</th>
                        <th>Avg Temperature (°C)</th>
                        <th>Avg Humidity (%)</th>
                        <th>Avg Battery (%)</th>
                    </tr>
                </thead>
                <tbody id="stats-body">
                    <tr>
                        <td colspan="5" style="text-align: center; color: #666; padding: 30px;">
                            Waiting for data...
                        </td>
                    </tr>
                </tbody>
            </table>
        </div>
        
        <div class="timestamp" id="last-update">Last updated: --:--:--</div>
        <div class="refresh-info">Dashboard auto-refreshes every 5 seconds</div>
    </div>
</body>
</html>
"""

def create_spark_session():
    """Create Spark session"""
    return SparkSession.builder \
        .appName("IoT Dashboard Backend") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/dashboard-checkpoint") \
        .getOrCreate()

def define_sensor_schema():
    """Define IoT sensor data schema"""
    return StructType([
        StructField("sensor_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", StringType(), True),
        StructField("battery_level", DoubleType(), True),
        StructField("signal_strength", DoubleType(), True)
    ])

def update_dashboard_data():
    """Update global dashboard data from Spark streaming"""
    global dashboard_data
    
    print("\n" + "="*60)
    print("Starting Spark Streaming Backend")
    print("="*60)
    
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("ERROR")
    
    sensor_schema = define_sensor_schema()
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "172.25.0.12:9092") \
        .option("subscribe", "iot-sensors") \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parse JSON
    parsed_df = kafka_df \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), sensor_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", col("timestamp").cast(TimestampType()))
    
    # Write to memory table for querying
    query1 = parsed_df \
        .writeStream \
        .outputMode("append") \
        .format("memory") \
        .queryName("live_sensors") \
        .start()
    
    # Detect alerts
    alerts_df = parsed_df \
        .filter(
            (col("temperature") > 35.0) | 
            (col("battery_level") < 20.0)
        )
    
    query2 = alerts_df \
        .writeStream \
        .outputMode("append") \
        .format("memory") \
        .queryName("live_alerts") \
        .start()
    
    print("\n✓ Streaming queries started")
    print("  - Sensors: live_sensors table")
    print("  - Alerts: live_alerts table")
    print("\nUpdating dashboard data every 3 seconds...")
    
    # Periodically update dashboard data
    while True:
        try:
            time.sleep(3)
            
            # Query live_sensors table
            sensors_df = spark.sql("""
                SELECT 
                    sensor_type,
                    COUNT(*) as count,
                    ROUND(AVG(temperature), 2) as avg_temp,
                    ROUND(AVG(humidity), 2) as avg_humidity,
                    ROUND(AVG(battery_level), 2) as avg_battery
                FROM live_sensors
                GROUP BY sensor_type
            """)
            
            # Query alerts table
            alerts_df = spark.sql("""
                SELECT *
                FROM live_alerts
                ORDER BY timestamp DESC
                LIMIT 10
            """)
            
            # Get total message count
            total_msg_df = spark.sql("SELECT COUNT(*) as total FROM live_sensors")
            total_messages = total_msg_df.collect()[0]['total'] if total_msg_df.count() > 0 else 0
            
            # Get alert count
            total_alert_df = spark.sql("SELECT COUNT(*) as total FROM live_alerts")
            total_alerts = total_alert_df.collect()[0]['total'] if total_alert_df.count() > 0 else 0
            
            # Get unique sensor count
            unique_sensors_df = spark.sql("SELECT COUNT(DISTINCT sensor_id) as total FROM live_sensors")
            active_sensors = unique_sensors_df.collect()[0]['total'] if unique_sensors_df.count() > 0 else 0
            
            # Update global data
            dashboard_data['total_messages'] = total_messages
            dashboard_data['total_alerts'] = total_alerts
            dashboard_data['active_sensors'] = active_sensors
            dashboard_data['system_health'] = 'Critical' if total_alerts > 10 else 'Warning' if total_alerts > 5 else 'Good'
            
            # Update sensor stats
            dashboard_data['sensor_stats'] = [
                {
                    'sensor_type': row['sensor_type'],
                    'count': row['count'],
                    'avg_temp': row['avg_temp'],
                    'avg_humidity': row['avg_humidity'],
                    'avg_battery': row['avg_battery']
                }
                for row in sensors_df.collect()
            ]
            
            # Update recent alerts
            dashboard_data['recent_alerts'] = [
                {
                    'sensor_id': row['sensor_id'],
                    'type': 'HIGH_TEMP' if row['temperature'] > 35 else 'LOW_BATTERY',
                    'location': row['location'],
                    'timestamp': str(row['timestamp'])
                }
                for row in alerts_df.collect()
            ]
            
        except Exception as e:
            print(f"Error updating dashboard: {e}")
            continue

# Flask routes
@app.route('/')
def index():
    """Main dashboard page"""
    return render_template_string(DASHBOARD_HTML)

@app.route('/api/metrics')
def get_metrics():
    """API endpoint for dashboard metrics"""
    return jsonify(dashboard_data)

def run_flask():
    """Run Flask server"""
    print("\n" + "="*60)
    print("Starting Flask Dashboard Server")
    print("="*60)
    print("\n🌐 Dashboard available at: http://localhost:4141")
    print("\nPress Ctrl+C to stop\n")
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)

if __name__ == "__main__":
    try:
        # Start Spark streaming in background thread
        spark_thread = Thread(target=update_dashboard_data, daemon=True)
        spark_thread.start()
        
        # Give Spark time to start
        time.sleep(5)
        
        # Start Flask in main thread
        run_flask()
        
    except KeyboardInterrupt:
        print("\n\nShutting down dashboard...")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback
        traceback.print_exc()
