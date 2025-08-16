Compiling the router.proto File
To compile the router.proto file into Python code, you can use the following command:

cd into src/v4vapp_backend_v2/lnd_grpc

```
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. router.proto
```
```
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. router.proto
```
```
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. invoices.proto
```


After that needed to change the import statements adding `from . import`
```
from . import lightning_pb2 as lightning__pb2
from . import router_pb2 as router__pb2
```

Use the Modify Import script

`modify_imports.py`

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


# UV

For uv-Managed Projects (Dependencies in pyproject.toml)uv projects use a uv.lock file to pin exact package versions based on the constraints in pyproject.toml. Upgrading updates the lockfile to the latest compatible versions (respecting your constraints, like >= or < bounds) and syncs the environment.Upgrade all packages in the lockfile:Run uv lock --upgrade to update the uv.lock file with the latest versions for all dependencies.
Then run uv sync to apply the changes to your project's virtual environment (installing or upgrading packages as needed).

Alternative (upgrade and sync in one step):Run uv sync --upgrade. This updates the lockfile and syncs the environment simultaneously.

Notes:If your pyproject.toml uses pinned versions (e.g., ==1.2.3), you'll need to relax them to ranges (e.g., >=1.2.3) for upgrades to take effect.
To upgrade a specific package only: uv lock --upgrade-package <package-name> (or uv sync --upgrade-package <package-name> to also sync).
Example:

uv sync --upgrade

