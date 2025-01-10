import pika
import json

class RabbitMQRouting:
    def __init__(self, host):
        self.host = host
        self.connection, self.channel = self.connect_to_rabbitmq()
        self.setup_queues()

    def connect_to_rabbitmq(self):
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
        channel = connection.channel()
        return connection, channel

    def setup_queues(self):
        for task in range(1, 6):
            self.channel.queue_declare(queue=f'task_{task}')

    def fetch_data(self, file_path):
        with open(file_path, 'r') as file:
            data = json.load(file)
        return [item for item in data if item.get('ready_for_processing', False)]

    def send_to_task_queue(self, items):
        for item in items:
            task_list = item.get('tasks', [])
            if task_list:
                first_task = task_list.pop(0)
                self.channel.basic_publish(exchange='', routing_key=f'task_{first_task}', body=json.dumps(item))
                print(f"Sent item {item['id']} to task_{first_task} queue")

    def close_connection(self):
        self.connection.close()
