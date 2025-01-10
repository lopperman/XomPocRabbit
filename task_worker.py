import pika
import json

class TaskWorker:
    def __init__(self, task_number, host):
        self.task_number = task_number
        self.connection, self.channel = self.connect_to_rabbitmq(host)
        self.channel.queue_declare(queue=f'task_{task_number}')

    def connect_to_rabbitmq(self, host):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=host))
        channel = connection.channel()
        return connection, channel

    def start_worker(self):
        def callback(ch, method, properties, body):
            item = json.loads(body)
            print(f"Task {self.task_number} processing item {item['id']}")

            next_tasks = item.get('tasks', [])
            if next_tasks:
                next_task = next_tasks.pop(0)
                self.channel.basic_publish(exchange='', routing_key=f'task_{next_task}', body=json.dumps(item))
                print(f"Item {item['id']} forwarded to task_{next_task}")
            else:
                print(f"Item {item['id']} processing complete.")

            ch.basic_ack(delivery_tag=method.delivery_tag)

        self.channel.basic_consume(queue=f'task_{self.task_number}', on_message_callback=callback, auto_ack=False)
        print(f"Task {self.task_number} worker started. Waiting for messages...")
        self.channel.start_consuming()

# Function to start a worker process
def start_worker_process(task_number):
    worker = TaskWorker(task_number, 'localhost')
    worker.start_worker()
