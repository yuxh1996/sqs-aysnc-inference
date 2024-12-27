import boto3
import os
import json
from concurrent.futures import ThreadPoolExecutor, wait
from scheduler.conf import schedulerConfig
import scheduler.api as api

# 设置 AWS 环境变量
os.environ['AWS_DEFAULT_REGION'] = schedulerConfig.get('aws', 'region')
queue_url = schedulerConfig.get('aws', 'queue_url')
worker_number = int(schedulerConfig.get('service', 'worker_number')) 

# 创建 SQS 客户端
sqs = boto3.client('sqs')

def process_message(message):
    retries = 2  # 设置重试次数
    while retries > 0:
        try:
            print("Processing:", message)
            body = json.loads(message['Body'])
            api = body['api']
            res = api.process_request(api, body)
            delete_message(message)  # 处理成功后删除消息
            print(f"Message processed and deleted: {message}")
            break  # 处理成功，退出循环
        except Exception as e:
            print(f"Error processing message: {e}, retries left: {retries-1}")
            retries -= 1
    if retries == 0:
        print("Failed to process message after retries, deleting it.")
        delete_message(message)

def delete_message(message):
    try:
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message['ReceiptHandle']
        )
        print(f"Message deleted: {message}")
    except Exception as e:
        print(f"Failed to delete message: {e}")

def receive_message():
    try:
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds=10
        )
        messages = response.get('Messages', [])
        return messages[0] if messages else None
    except Exception as e:
        print(f"Failed to receive message: {e}")
        return None

def _receive_and_process_message():
    message = receive_message()
    if message:
        process_message(message)
    else:
        print("No message received. Waiting for next poll...")

def receiveAndProcess():
    with ThreadPoolExecutor(max_workers=worker_number) as executor:
        while True:
            # 提交两个初始任务
            futures = [executor.submit(_receive_and_process_message) for _ in range(worker_number)]

            # 等待任何一个任务完成
            done, not_done = wait(futures, return_when='FIRST_COMPLETED')

            # 移除已完成的任务并提交新任务以保持两个并行任务
            for future in done:
                futures.remove(future)
                futures.append(executor.submit(_receive_and_process_message))
