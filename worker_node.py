import grpc
from concurrent import futures
import time
import sys
import threading
import uuid
import socket
import cloudpickle 
# Import generated classes
import distro_pb2
import distro_pb2_grpc

# --- CONFIG ---
HEAD_NODE_ADDRESS = 'localhost:50051'
MY_PORT = 50052 # In a real system, this would be random or assigned
MY_ADDRESS = f'localhost:{MY_PORT}'

# Add this global dictionary to store results
worker_results = {}

class WorkerService(distro_pb2_grpc.WorkerServiceServicer):
    def ExecuteTask(self, request, context):
        print(f"[Worker] Received Task ID: {request.task_id}")
        try:
            # 1. Deserialize
            func = cloudpickle.loads(request.function_payload)
            args = cloudpickle.loads(request.args_payload)
            
            # 2. Execute
            print(f"[Worker] Running function...")
            result = func(*args)
            
            # 3. STORE RESULT (New!)
            result_payload = cloudpickle.dumps(result)
            worker_results[request.task_id] = result_payload
            print(f"✅ Result computed and stored: {result}")
            
            return distro_pb2.TaskStatus(task_id=request.task_id, success=True)
        except Exception as e:
            print(f"❌ Execution Failed: {e}")
            return distro_pb2.TaskStatus(task_id=request.task_id, success=False, error_message=str(e))

    def GetResult(self, request, context):
        """Client calls this to fetch the data."""
        task_id = request.result_object_id
        
        if task_id in worker_results:
            return distro_pb2.ResultResponse(
                ready=True,
                result=worker_results[task_id]
            )
        else:
            return distro_pb2.ResultResponse(ready=False)

def heartbeat_loop(stub, worker_id):
    """Keeps telling the Head Node we are alive."""
    while True:
        try:
            # Fake resources for now
            resources = distro_pb2.Resources(num_cpus=4, num_gpus=0, memory_mb=1024)
            stub.SendHeartbeat(distro_pb2.Heartbeat(
                worker_id=worker_id,
                available_resources=resources
            ))
        except grpc.RpcError:
            print("[Worker] Heartbeat failed! Is Head Node dead?")
        
        time.sleep(2)

def serve():
    # 1. Start the Worker gRPC Server (So Head can talk to us)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=5))
    distro_pb2_grpc.add_WorkerServiceServicer_to_server(WorkerService(), server)
    server.add_insecure_port(f'[::]:{MY_PORT}')
    server.start()
    print(f"[Worker] Listening for tasks on port {MY_PORT}")

    # 2. Connect to the Head Node (To register ourselves)
    channel = grpc.insecure_channel(HEAD_NODE_ADDRESS)
    stub = distro_pb2_grpc.SchedulerServiceStub(channel)

    print(f"[Worker] Registering with Head Node at {HEAD_NODE_ADDRESS}...")
    try:
        # Initial Registration
        response = stub.RegisterWorker(distro_pb2.WorkerRegistration(
            address=MY_ADDRESS,
            total_resources=distro_pb2.Resources(num_cpus=8, num_gpus=1, memory_mb=16000)
        ))
        
        if response.success:
            print(f"[Worker] SUCCESS! Assigned ID: {response.worker_id}")
            
            # 3. Start Heartbeat in background
            hb_thread = threading.Thread(target=heartbeat_loop, args=(stub, response.worker_id))
            hb_thread.daemon = True
            hb_thread.start()
            
            # Keep main thread alive to serve tasks
            server.wait_for_termination()
            
    except grpc.RpcError as e:
        print(f"[Worker] Could not connect to Head Node: {e}")

if __name__ == '__main__':
    serve()