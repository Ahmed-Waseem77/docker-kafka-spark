#!/usr/bin/env python3
"""
Lab 4 Setup Verification Script
Run this to verify your environment is correctly configured
"""

import sys
import subprocess
import os

def print_header(text):
    print(f"\n{'='*60}")
    print(f"  {text}")
    print('='*60)

def check_command(command, name):
    """Check if a command exists"""
    try:
        subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
        print(f"✅ {name} is installed")
        return True
    except (subprocess.CalledProcessError, FileNotFoundError):
        print(f"❌ {name} is NOT installed")
        return False

def check_env_var(var_name):
    """Check if environment variable is set"""
    value = os.environ.get(var_name)
    if value:
        print(f"✅ {var_name} is set: {value}")
        return True
    else:
        print(f"❌ {var_name} is NOT set")
        return False

def check_python_package(package_name):
    """Check if Python package is installed"""
    try:
        __import__(package_name.replace('-', '_'))
        print(f"✅ {package_name} is installed")
        return True
    except ImportError:
        print(f"❌ {package_name} is NOT installed")
        return False

def check_kafka_running():
    """Check if Kafka is running"""
    try:
        result = subprocess.run(['jps'], capture_output=True, text=True)
        if 'Kafka' in result.stdout and 'QuorumPeerMain' in result.stdout:
            print("✅ Kafka and Zookeeper are running")
            return True
        elif 'Kafka' in result.stdout:
            print("⚠️  Kafka is running but Zookeeper is not")
            return False
        elif 'QuorumPeerMain' in result.stdout:
            print("⚠️  Zookeeper is running but Kafka is not")
            return False
        else:
            print("❌ Kafka and Zookeeper are NOT running")
            print("   Start them with:")
            print("   $KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $KAFKA_HOME/config/zookeeper.properties")
            print("   $KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_HOME/config/server.properties")
            return False
    except FileNotFoundError:
        print("❌ 'jps' command not found (Java not in PATH)")
        return False

def main():
    print("\n" + "🔍 Lab 4 Setup Verification" + "\n")
    
    all_checks = []
    
    # Check Java
    print_header("Java Installation")
    all_checks.append(check_command(['java', '-version'], 'Java'))
    
    # Check Python
    print_header("Python Environment")
    python_version = sys.version_info
    print(f"✅ Python version: {python_version.major}.{python_version.minor}.{python_version.micro}")
    
    if python_version.major == 3 and python_version.minor >= 8:
        print("✅ Python version is compatible")
        all_checks.append(True)
    else:
        print("❌ Python 3.8+ required")
        all_checks.append(False)
    
    # Check environment variables
    print_header("Environment Variables")
    all_checks.append(check_env_var('JAVA_HOME'))
    all_checks.append(check_env_var('KAFKA_HOME'))
    all_checks.append(check_env_var('SPARK_HOME'))
    
    # Check Python packages
    print_header("Python Dependencies")
    packages = [
        'kafka',
        'pyspark',
        'flask',
        'plotly',
        'confluent_kafka'
    ]
    for package in packages:
        all_checks.append(check_python_package(package))
    
    # Check Kafka
    print_header("Kafka Services")
    kafka_running = check_kafka_running()
    
    # Summary
    print_header("Verification Summary")
    passed = sum(all_checks)
    total = len(all_checks)
    
    print(f"\nPassed: {passed}/{total} checks")
    
    if passed == total:
        print("\n✅ All checks passed! Your environment is ready.")
        if not kafka_running:
            print("\n⚠️  Note: Kafka is not running. Start it before running the lab.")
    else:
        print("\n❌ Some checks failed. Please fix the issues above.")
        print("\nCommon fixes:")
        print("  - Add environment variables to ~/.bashrc")
        print("  - Activate virtual environment: source spark_env/bin/activate")
        print("  - Install missing packages: pip install <package-name>")
    
    return passed == total

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
