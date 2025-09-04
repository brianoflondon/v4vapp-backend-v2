#!/usr/bin/env python3
"""
Admin Test Runner

Simple script to run all admin interface tests.
"""

import subprocess
import sys
from pathlib import Path


def run_admin_tests():
    """Run all admin interface tests"""
    project_root = Path(__file__).parent

    # Change to project root
    import os

    os.chdir(project_root)

    # Use virtual environment's pytest
    pytest_cmd = str(project_root / ".venv" / "bin" / "pytest")

    # Test files to run
    test_files = ["tests/test_admin_interface.py", "tests/test_admin_templates.py"]

    print("🚀 Running Admin Interface Tests")
    print("=" * 50)

    all_passed = True

    for test_file in test_files:
        if Path(test_file).exists():
            print(f"\n📋 Running {test_file}")
            print("-" * 30)

            # Run pytest on the specific file
            result = subprocess.run(
                [pytest_cmd, test_file, "-v", "--tb=short", "--color=yes"], capture_output=False
            )

            if result.returncode != 0:
                all_passed = False
                print(f"❌ {test_file} failed")
            else:
                print(f"✅ {test_file} passed")
        else:
            print(f"⚠️  {test_file} not found")
            all_passed = False

    print("\n" + "=" * 50)
    if all_passed:
        print("🎉 All admin tests passed!")
        return 0
    else:
        print("💥 Some admin tests failed!")
        return 1


def run_specific_test(test_name):
    """Run a specific test function"""
    project_root = Path(__file__).parent
    import os

    os.chdir(project_root)

    # Use virtual environment's pytest
    pytest_cmd = str(project_root / ".venv" / "bin" / "pytest")

    print(f"🎯 Running specific test: {test_name}")

    result = subprocess.run(
        [pytest_cmd, "-k", test_name, "-v", "--tb=short", "--color=yes"], capture_output=False
    )

    return result.returncode


if __name__ == "__main__":
    if len(sys.argv) > 1:
        # Run specific test
        test_name = sys.argv[1]
        sys.exit(run_specific_test(test_name))
    else:
        # Run all admin tests
        sys.exit(run_admin_tests())
