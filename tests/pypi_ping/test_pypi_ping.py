from pathlib import Path

import toml

# def parse_pyproject_toml(file_path):
#     # Load the toml file
#     with open(file_path, "r") as file:
#         data = toml.load(file)

#     # List to store PyPI projects
#     pypi_projects = []

#     # Extract dependencies from the 'project' table
#     project = data.get("tool", {})
#     dependencies = data.get("[tool.poetry.dependencies]", [])
#     if dependencies:
#         pypi_projects.extend(dependencies)

#     # Check for optional dependencies
#     optional_dependencies = project.get("tool.poetry.group.dev.dependencies", {})
#     for group, deps in optional_dependencies.items():
#         pypi_projects.extend(deps)

#     # Remove any duplicates and filter out non-PyPI dependencies
#     # Here we assume all entries are PyPI packages unless they contain markers or are URL dependencies
#     pypi_projects = [
#         dep
#         for dep in set(pypi_projects)
#         if not (";" in dep or "@" in dep or "git+" in dep)
#     ]

#     return pypi_projects


def parse_pyproject_toml(file_path: Path) -> dict:
    # Read and parse the pyproject.toml file
    with open(file_path, "r") as f:
        pyproject_data = toml.load(f)

    # Extract dependencies
    dependencies = (
        pyproject_data.get("tool", {}).get("poetry", {}).get("dependencies", {})
    )
    dev_dependencies = (
        pyproject_data.get("tool", {})
        .get("poetry", {})
        .get("group", {})
        .get("dev", {})
        .get("dependencies", {})
    )

    # Combine dependencies and dev dependencies into a single dictionary
    all_dependencies = {**dependencies, **dev_dependencies}

    return all_dependencies


def test_pyproject_read():
    # Example usage
    file_path = Path("" + "pyproject.toml")  # Replace with your actual file path

    print(file_path.resolve())
    pypi_packages = parse_pyproject_toml(file_path)

    print("PyPI Packages Found:")
    for package, version in pypi_packages.items():
        print(package, version)
