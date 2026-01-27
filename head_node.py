import grpc
from concurrent import futures
import time
import uuid

# Import the generated classes
import distro_pb2
import distro_pb2_grpc

class SchedulerService(distro_pb2_grpc.SchedulerServiceServicer):
    def __init__(self):
        # State: Keeps track of all active workers
        # Format: { "worker_id": { "address": "ip:port", "resources": {...}, "last_beat": timestamp } }
        self.workers = {}
        self.tasks = []

    def RegisterWorker(self, request, context):
        """Worker calls this to join the cluster."""
        worker_id = str(uuid.uuid4())
        print(f"[Head] New Worker Request from {request.address}")
        
        # Store worker info
        self.workers[worker_id] = {
            "address": request.address,
            "resources": request.total_resources,
            "last_beat": time.time()
        }
        
        print(f"[Head] Worker registered: {worker_id}")
        return distro_pb2.RegistrationResponse(worker_id=worker_id, success=True)

    def SendHeartbeat(self, request, context):
        """Worker calls this to prove it's alive."""
        if request.worker_id in self.workers:
            self.workers[request.worker_id]["last_beat"] = time.time()
            # In a real system, we'd update available resources here
            return distro_pb2.Empty()
        else:
            # If we don't know this worker, tell them? (For now, just ignore)
            return distro_pb2.Empty()

    def SubmitTask(self, request, context):
        """Client calls this to run a function."""
        task_id = str(uuid.uuid4())
        print(f"[Head] Received Task: {request.function_name} (ID: {task_id})")
        
        # 1. SCHEDULING: Pick a worker (Simple Round-Robin or Random)
        if not self.workers:
            print("[Head] ERROR: No workers available!")
            return distro_pb2.TaskSubmissionResponse(task_id=task_id, result_object_id="")

        # Just pick the first available worker for this MVP
        worker_id, worker_info = list(self.workers.items())[0]
        worker_address = worker_info['address']
        print(f"[Head] Assigning task to Worker {worker_id} at {worker_address}")

        # 2. DISPATCH: Connect to the Worker and send the payload
        try:
            # Connect to the specific worker
            worker_channel = grpc.insecure_channel(worker_address)
            worker_stub = distro_pb2_grpc.WorkerServiceStub(worker_channel)
            
            # Forward the task
            worker_stub.ExecuteTask(distro_pb2.TaskDefinition(
                task_id=task_id,
                function_payload=request.function_payload,
                args_payload=request.args_payload
            ))
            print(f"[Head] Task sent to worker successfully.")
            
        except Exception as e:
            print(f"[Head] Failed to send task to worker: {e}")

        # 3. Return the Future ID to the client
        # ... inside head_node.py ...
        
        # 3. Return the info to the client
        return distro_pb2.TaskSubmissionResponse(
            task_id=task_id,
            result_object_id=task_id,
            worker_address=worker_address  # <--- MAKE SURE THIS LINE EXISTS
        )

def serve():
    print("[Head] Starting Head Node Scheduler...")
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    distro_pb2_grpc.add_SchedulerServiceServicer_to_server(SchedulerService(), server)
    
    # Listen on port 50051
    server.add_insecure_port('[::]:50051')
    server.start()
    print("[Head] Scheduler listening on port 50051")
    
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        print("\n[Head] Shutting down...")

if __name__ == '__main__':
    serve()