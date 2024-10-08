from rq import Queue
from redis import Redis
from add_worker import add

# Set up the Redis connection
redis_conn = Redis(host='localhost', port=6379, db=0)

# Create a Queue object
queue = Queue(connection=redis_conn)

# Enqueue an addition job
job = queue.enqueue(add, 5, 7)

print(f"Job ID: {job.id}")