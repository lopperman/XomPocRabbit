from rabbitmq_routing import RabbitMQRouting
from task_worker import start_worker_process
import multiprocessing

# Configuration
rabbitmq_host = 'localhost'
data_file = 'data.json'

# Main function to start the RabbitMQ routing and task workers
def main():
    # Step 1: Route items from data.json to task queues
    router = RabbitMQRouting(rabbitmq_host)
    items = router.fetch_data(data_file)
    print(f"Fetched {len(items)} items ready for processing.")
    router.send_to_task_queue(items)
    router.close_connection()

    # Step 2: Start task workers using multiprocessing
    processes = []
    for task_number in range(1, 6):
        process = multiprocessing.Process(target=start_worker_process, args=(task_number,))
        processes.append(process)
        process.start()

    # Wait for all processes to complete
    for process in processes:
        process.join()

if __name__ == "__main__":
    main()
