# Platform-specific CA certificate paths
import platform
import os


def get_ca_bundle_path():
    """Get the CA bundle path for different platforms"""
    system = platform.system()
    possible_paths = []

    if system == "Darwin":  # macOS
        possible_paths = [
            "/etc/ssl/cert.pem",
            "/usr/local/etc/openssl/cert.pem",
            "/System/Library/OpenSSL/cert.pem",
        ]
    elif system == "Linux":
        possible_paths = [
            "/etc/ssl/certs/ca-certificates.crt",
            "/etc/pki/tls/certs/ca-bundle.crt",
            "/etc/ssl/ca-bundle.pem",
        ]

    for path in possible_paths:
        if os.path.exists(path):
            return path

    return None

