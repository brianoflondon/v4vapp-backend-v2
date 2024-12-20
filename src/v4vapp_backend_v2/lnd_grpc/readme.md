Compiling the router.proto File
To compile the router.proto file into Python code, you can use the following command:

```
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. router.proto
```
```
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. router.proto
```

After that needed to change the import statements adding `from . import`
```
from . import lightning_pb2 as lightning__pb2
from . import router_pb2 as router__pb2
```

Explanation
-I.: Specifies the directory in which to search for imports. The . means the current directory.
--python_out=.: Specifies the directory where the generated Python code for the messages should be placed. The . means the current directory.
--grpc_python_out=.: Specifies the directory where the generated Python code for the gRPC services should be placed. The . means the current directory.
router.proto: The path to the .proto file to be compiled.
Generated Files
After running the protoc command, you will have two generated files:

router_pb2.py: Contains the generated code for the messages defined in the .proto file.
router_pb2_grpc.py: Contains the generated code for the gRPC services defined in the .proto file.
Example Usage
Here's an example of how you can use the generated code in your Python application: