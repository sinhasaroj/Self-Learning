from rq import Worker, Queue, Connection
import redis

# Set up the Redis connection
redis_conn = redis.Redis(host='localhost', port=6379, db=0)

# Create a Queue object
queue = Queue(connection=redis_conn)

# Function to perform addition
def add(x, y):
    result = x + y
    print(f"Adding {x} + {y} = {result}")
    return result

if __name__ == '__main__':
    with Connection(redis_conn):
        worker = Worker([queue], connection=redis_conn)
        worker.work()