import grpc
import cloudpickle
import time
import distro_pb2
import distro_pb2_grpc

HEAD_NODE_ADDRESS = 'localhost:50051'

class DistroClient:
    def __init__(self):
        self.channel = grpc.insecure_channel(HEAD_NODE_ADDRESS)
        self.stub = distro_pb2_grpc.SchedulerServiceStub(self.channel)
        
        # Store where each task lives: { task_id: worker_address }
        self.task_map = {} 

    def submit(self, func, *args, **kwargs):
        try:
            # Pickle the function logic
            func_payload = cloudpickle.dumps(func)
            args_payload = cloudpickle.dumps(args)
        except Exception as e:
            print(f"[Client] Serialization Error: {e}")
            return None
        
        # Send to Head Node
        response = self.stub.SubmitTask(distro_pb2.TaskRequest(
            function_name=func.__name__,
            function_payload=func_payload,
            args_payload=args_payload,
            resources_required=distro_pb2.Resources(num_cpus=1)
        ))
        
        # Remember which worker got the job so we can fetch the result later
        self.task_map[response.task_id] = response.worker_address
        
        print(f"[Client] Job submitted! Task ID: {response.task_id}")
        return response.task_id

    def get(self, task_id):
        """Fetches the result from the specific worker."""
        if task_id not in self.task_map:
            print(f"[Client] Error: Unknown task ID {task_id}")
            return None
            
        worker_addr = self.task_map[task_id]
        print(f"[Client] Connecting to Worker at {worker_addr} to fetch result...")
        
        # Connect directly to the worker
        worker_channel = grpc.insecure_channel(worker_addr)
        worker_stub = distro_pb2_grpc.WorkerServiceStub(worker_channel)
        
        # Poll until ready
        while True:
            try:
                response = worker_stub.GetResult(distro_pb2.ResultRequest(result_object_id=task_id))
                if response.ready:
                    return cloudpickle.loads(response.result)
            except grpc.RpcError as e:
                print(f"[Client] Connection error to worker: {e}")
                return None
            
            # Wait a bit if not ready
            time.sleep(0.5)

# Global instance
_client = DistroClient()

# --- The Helper Class ---
class RemoteFunction:
    def __init__(self, func):
        self._func = func  # The raw, clean function
        
    def remote(self, *args, **kwargs):
        # We submit the raw function, not this wrapper object!
        return _client.submit(self._func, *args, **kwargs)
        
    def __call__(self, *args, **kwargs):
        # Allow running locally as normal
        return self._func(*args, **kwargs)

def remote(func):
    return RemoteFunction(func)


# USER CODE STARTS HERE

if __name__ == "__main__":
    print("--- DISTRO AI CLIENT ---")
    
    # 1. Define a function to run on the cluster
    @remote
    def heavy_computation(x, y):
        # Imports must happen INSIDE the function because 
        # this runs on a machine that might not have imported them yet.
        import time 
        print(f"I am running on a worker! Calculating {x} + {y}")
        time.sleep(1) 
        return x + y

    print("Submitting task to cluster...")
    task_id = heavy_computation.remote(10, 20)
    
    if task_id:
        print("Waiting for result...")
        result = _client.get(task_id)
        
        print(f"🚀 FINAL RESULT: {result}")
