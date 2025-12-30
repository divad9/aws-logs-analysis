import json
import boto3
import os
import base64
from datetime import datetime
from decimal import Decimal
import uuid

# Initialize clients
dynamodb = boto3.resource('dynamodb')
sns = boto3.client('sns')

# Environment variables
ANOMALIES_TABLE = os.environ.get('ANOMALIES_TABLE', 'LogGuardAnomalies')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN')

# Anomaly detection thresholds
ERROR_THRESHOLD = 5  
FAILED_LOGIN_THRESHOLD = 3  


def lambda_handler(event, context):
   # function to handle incoming Kinesis stream events 
    
    print(f"Processing {len(event['Records'])} records")
    
    anomalies_detected = []
    error_count = 0
    failed_login_count = 0
    
    
    for record in event['Records']:
        # Decode the data
        payload = base64.b64decode(record['kinesis']['data']).decode('utf-8')
        
        try:
            log_entry = json.loads(payload)
            
            # Detect anomalies
            anomaly = detect_anomaly(log_entry)
            
            if anomaly:
                anomalies_detected.append(anomaly)
                
                # Count specific types
                if anomaly['type'] == 'ERROR_SPIKE':
                    error_count += 1
                elif anomaly['type'] == 'FAILED_LOGIN':
                    failed_login_count += 1
        
        except json.JSONDecodeError as e:
            print(f"Failed to parse log: {e}")
            continue
    
    # Store anomalies in DynamoDB
    if anomalies_detected:
        store_anomalies(anomalies_detected)
        
        # Send alert if critical
        critical_anomalies = [a for a in anomalies_detected if a['severity'] in ['HIGH', 'CRITICAL']]
        
        if critical_anomalies:
            send_alert(critical_anomalies)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed': len(event['Records']),
            'anomalies_detected': len(anomalies_detected),
            'critical_anomalies': len([a for a in anomalies_detected if a['severity'] in ['HIGH', 'CRITICAL']])
        })
    }


def detect_anomaly(log_entry):
   # Function to Detect anomalies based on log entry content
    
    # Get log details
    level = log_entry.get('level', '').upper()
    message = log_entry.get('message', '')
    source = log_entry.get('source', 'unknown')
    
    # Rule 1: Error logs
    if level == 'ERROR':
        return {
            'anomaly_id': str(uuid.uuid4()),
            'timestamp': int(datetime.now().timestamp()),
            'type': 'ERROR_SPIKE',
            'severity': 'MEDIUM',
            'source': source,
            'message': message,
            'log_level': level,
            'details': log_entry
        }
    
    # Rule 2: Failed login attempts
    if 'failed login' in message.lower() or 'authentication failed' in message.lower():
        return {
            'anomaly_id': str(uuid.uuid4()),
            'timestamp': int(datetime.now().timestamp()),
            'type': 'FAILED_LOGIN',
            'severity': 'HIGH',
            'source': source,
            'message': message,
            'log_level': level,
            'details': log_entry
        }
    
    # Rule 3: Critical level logs
    if level == 'CRITICAL':
        return {
            'anomaly_id': str(uuid.uuid4()),
            'timestamp': int(datetime.now().timestamp()),
            'type': 'CRITICAL_ERROR',
            'severity': 'CRITICAL',
            'source': source,
            'message': message,
            'log_level': level,
            'details': log_entry
        }
    
    # Rule 4: Database connection failures
    if 'database' in message.lower() and ('timeout' in message.lower() or 'connection' in message.lower()):
        return {
            'anomaly_id': str(uuid.uuid4()),
            'timestamp': int(datetime.now().timestamp()),
            'type': 'DATABASE_ISSUE',
            'severity': 'HIGH',
            'source': source,
            'message': message,
            'log_level': level,
            'details': log_entry
        }
    
    return None


def store_anomalies(anomalies):
    # store detected anomalies in DynamoDB
    
    table = dynamodb.Table(ANOMALIES_TABLE)
    
    for anomaly in anomalies:
        try:
            # Convert to DynamoDB-compatible format
            item = {
                'anomaly_id': anomaly['anomaly_id'],
                'timestamp': anomaly['timestamp'],
                'type': anomaly['type'],
                'severity': anomaly['severity'],
                'source': anomaly['source'],
                'message': anomaly['message'],
                'log_level': anomaly['log_level'],
                'details': json.dumps(anomaly['details'])
            }
            
            table.put_item(Item=item)
            print(f" Stored anomaly: {anomaly['type']} - {anomaly['severity']}")
            
        except Exception as e:
            print(f" Failed to store anomaly: {e}")


def send_alert(anomalies):
    #send alert via SNS for critical anomalies
    
    if not SNS_TOPIC_ARN:
        print(" SNS_TOPIC_ARN not configured")
        return
    
    # Build alert message
    message = f"""
 LogGuard Alert - {len(anomalies)} Critical Anomalies Detected
{'='*60}

"""
    
    for anomaly in anomalies[:5]:  # Limit to 5 in email
        message += f"""
Type: {anomaly['type']}
Severity: {anomaly['severity']}
Source: {anomaly['source']}
Message: {anomaly['message']}
Time: {datetime.fromtimestamp(anomaly['timestamp']).strftime('%Y-%m-%d %H:%M:%S')}
---
"""
    
    if len(anomalies) > 5:
        message += f"\n... and {len(anomalies) - 5} more anomalies\n"
    
    message += f"\n🔍 Check your LogGuard dashboard for full details"
    
    try:
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f" LogGuard Alert - {len(anomalies)} Critical Issues",
            Message=message
        )
        print(f" Alert sent for {len(anomalies)} anomalies")
    except Exception as e:
        print(f" Failed to send alert: {e}")