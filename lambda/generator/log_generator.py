import json
import boto3
import random
from datetime import datetime

kinesis = boto3.client('kinesis')

STREAM_NAME = 'LogGuardStream'

# Sample log sources
SOURCES = ['web-app', 'api-server', 'database', 'auth-service', 'payment-service']

# Sample log messages
NORMAL_MESSAGES = [
    'User logged in successfully',
    'API request processed',
    'Database query executed',
    'Cache hit',
    'File uploaded successfully'
]

ERROR_MESSAGES = [
    'Database connection timeout',
    'Failed to process payment',
    'API rate limit exceeded',
    'File not found',
    'Memory allocation failed'
]

CRITICAL_MESSAGES = [
    'Database connection pool exhausted',
    'Out of memory - service crashing',
    'Failed login attempt detected',
    'Authentication failed - potential breach'
]


def lambda_handler(event, context):
    # Lambda function to generate and send logs to Kinesis
    
    # Number of logs to generate
    num_logs = event.get('num_logs', 10)
    include_anomalies = event.get('include_anomalies', True)
    
    logs_sent = 0
    
    for i in range(num_logs):
        log_entry = generate_log(include_anomalies and i % 5 == 0)
        
        # Send to Kinesis
        try:
            kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(log_entry),
                PartitionKey=log_entry['source']
            )
            logs_sent += 1
        except Exception as e:
            print(f"Failed to send log: {e}")
    
    print(f" Sent {logs_sent} logs to Kinesis")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'logs_sent': logs_sent,
            'stream': STREAM_NAME
        })
    }


def generate_log(include_anomaly=False):
    # Generate a single log entry, optionally with an anomaly
    
    source = random.choice(SOURCES)
    
    if include_anomaly:
        # Generate anomalous log
        if random.random() < 0.5:
            level = 'ERROR'
            message = random.choice(ERROR_MESSAGES)
        else:
            level = 'CRITICAL'
            message = random.choice(CRITICAL_MESSAGES)
    else:
        # Generate normal log
        level = random.choice(['INFO', 'INFO', 'INFO', 'WARNING'])
        message = random.choice(NORMAL_MESSAGES)
    
    return {
        'timestamp': datetime.now().isoformat(),
        'level': level,
        'source': source,
        'message': message,
        'user_id': f'user_{random.randint(1000, 9999)}',
        'request_id': f'req_{random.randint(100000, 999999)}'
    }