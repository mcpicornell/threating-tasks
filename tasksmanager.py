import argparse
import datetime
import os
import sqlite3
import threading
import queue
import logging
import time

logging.basicConfig(level=logging.INFO)

logger = logging.getLogger(__name__)


# Definition of the Task class
class Task:
    def __init__(self, task_id):
        self.task_id = task_id
        self.created_at = datetime.datetime.now()
        self.completed_at = None

    def execute(self):
        try:
            self.completed_at = datetime.datetime.now()
            db_execute("UPDATE tasks SET status = 'completed', completed_at = ? WHERE id = ?",
                       (self.completed_at, self.task_id))
        except Exception as e:
            logger.error(f"Error executing task {self.task_id}: {e}")


# Worker function to execute tasks in threads
def worker(task_queue):
    while not task_queue.empty():
        task = task_queue.get()
        task.execute()
        task_queue.task_done()


# Function to create and execute tasks
def create_and_execute_tasks(num_tasks, num_threads):
    # Create a queue to hold the tasks
    task_queue = queue.Queue()

    for i in range(1, num_tasks + 1):
        task = Task(i)
        task_queue.put(task)
        connection = sqlite3.connect('tasks.db')
        cursor = connection.cursor()
        cursor.execute("INSERT INTO tasks (name, status, created_at) VALUES (?, ?, ?)",
                       (f"Task {i}", 'pending', datetime.datetime.now()))
        connection.commit()
        connection.close()

    threads = []
    for _ in range(num_threads):
        thread = threading.Thread(target=worker, args=(task_queue,))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()
    logger.info("All tasks processed")


def report_from_bd():
    connection = sqlite3.connect('tasks.db')
    cursor = connection.cursor()
    cursor.execute("SELECT * FROM tasks")
    tasks = cursor.fetchall()
    connection.close()
    for task in tasks:
        logger.info(task)
        time.sleep(0.001)


def db_execute(query, args=()):
    connection = sqlite3.connect('tasks.db')
    cursor = connection.cursor()
    cursor.execute(query, args)
    connection.commit()
    connection.close()


def create_db():
    try:
        os.remove('tasks.db')
    except FileNotFoundError:
        pass

    db_execute('''CREATE TABLE IF NOT EXISTS tasks (
                        id INTEGER PRIMARY KEY,
                        name TEXT,
                        status TEXT,
                        created_at TEXT,
                        completed_at TEXT)''')


def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--batch_size', type=int, default=10, help='Batch size for updating tasks in the database')
    args = parser.parse_args()

    # Create db if not exists
    create_db()
    create_and_execute_tasks(1000, args.batch_size)

    report_from_bd()


if __name__ == "__main__":
    main()
