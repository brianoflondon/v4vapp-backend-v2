"""
This script modifies import statements in generated Python files within a
specified directory.

python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. --mypy_out=. lightning.proto
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. --mypy_out=. router.proto

Functions:
    modify_imports(file_path):
        Reads a Python file, modifies specific import statements,
        and writes the changes back to the file.

    main():
        Defines the directory containing the generated files and modifies the
        import statements in all .py files within that directory.

Usage:
    Run this script directly to modify import statements in
    all generated .py files in the specified directory.
"""

import os
import re


def modify_imports(file_path):
    with open(file_path, "r") as file:
        lines = file.readlines()

    with open(file_path, "w") as file:
        for line in lines:
            # Match lines like 'import module_pb2 as module__pb2'
            match = re.match(r"import (\w+_pb2) as (\w+__pb2)", line)
            if match:
                print(f"Modifying import statement in {file_path}")
                print(f"Original: {line.strip()}")
                print(f"Modified: from . import {match.group(1)} as {match.group(2)}")
                print()
                module_name = match.group(1)
                alias = match.group(2)
                file.write(f"from . import {module_name} as {alias}\n")
            else:
                file.write(line)


def main():
    # Define the directory containing the generated files
    print("Modifying import statements in generated Python files...")
    directory = "src/v4vapp_backend_v2/lnd_grpc"

    # Modify the import statements in all generated .py files
    for root, _, files in os.walk(directory):
        print(f"Processing directory... {root}")
        for file_name in files:
            print(f"Processing {file_name}...")
            if file_name.endswith(".py"):
                file_path = os.path.join(root, file_name)
                modify_imports(file_path)


if __name__ == "__main__":
    main()
