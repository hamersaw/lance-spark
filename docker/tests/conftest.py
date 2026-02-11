"""
Shared fixtures for Lance-Spark integration tests.

Fixtures defined here are available to all test modules under docker/tests/.
"""

import subprocess
import time
import urllib.request
import urllib.error

import pytest

# ---------------------------------------------------------------------------
# Azurite (Azure Blob Storage emulator) configuration
# ---------------------------------------------------------------------------
AZURITE_BLOB_PORT = 10100  # Avoid conflict with Thrift server on 10000
AZURITE_ACCOUNT_NAME = "devstoreaccount1"
AZURITE_ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsu"
    "Fq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw=="
)
AZURITE_CONTAINER = "lance-test"


@pytest.fixture(scope="session")
def azurite():
    """Start Azurite blob service, create the test container, and yield config.

    This fixture is **not** autouse — it only runs when a test explicitly
    depends on it (directly or transitively).  Running
    ``pytest test_lance_spark.py`` alone will never start Azurite.
    """
    proc = subprocess.Popen(
        [
            "azurite-blob",
            "--blobHost", "0.0.0.0",
            "--blobPort", str(AZURITE_BLOB_PORT),
            "--skipApiVersionCheck",
            "--silent",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    # Poll until the blob service is healthy (up to 30 s).
    # Azurite returns HTTP 400 on the root URL, which is fine — any HTTP
    # response means the server is up.
    blob_url = f"http://127.0.0.1:{AZURITE_BLOB_PORT}"
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(blob_url, timeout=1)
            break
        except urllib.error.HTTPError:
            break  # Server is responding (400 from Azurite is expected)
        except (urllib.error.URLError, OSError):
            if proc.poll() is not None:
                raise RuntimeError("azurite-blob exited unexpectedly")
            time.sleep(0.5)
    else:
        proc.terminate()
        raise RuntimeError("azurite-blob did not become healthy within 30 s")

    # Create the blob container using the Azure SDK.
    from azure.storage.blob import BlobServiceClient

    conn_str = (
        f"DefaultEndpointsProtocol=http;"
        f"AccountName={AZURITE_ACCOUNT_NAME};"
        f"AccountKey={AZURITE_ACCOUNT_KEY};"
        f"BlobEndpoint=http://127.0.0.1:{AZURITE_BLOB_PORT}/{AZURITE_ACCOUNT_NAME};"
    )
    blob_service = BlobServiceClient.from_connection_string(conn_str)
    blob_service.create_container(AZURITE_CONTAINER)

    yield {
        "account_name": AZURITE_ACCOUNT_NAME,
        "account_key": AZURITE_ACCOUNT_KEY,
        "container": AZURITE_CONTAINER,
        "port": AZURITE_BLOB_PORT,
        "endpoint": f"http://127.0.0.1:{AZURITE_BLOB_PORT}/{AZURITE_ACCOUNT_NAME}",
    }

    proc.terminate()
    proc.wait(timeout=5)


# ---------------------------------------------------------------------------
# MinIO (S3-compatible storage) configuration
# ---------------------------------------------------------------------------
MINIO_PORT = 9000
MINIO_CONSOLE_PORT = 9001
MINIO_ROOT_USER = "minioadmin"
MINIO_ROOT_PASSWORD = "minioadmin"
MINIO_BUCKET = "lance-test"


@pytest.fixture(scope="session")
def minio():
    """Start MinIO server, create the test bucket, and yield config.

    This fixture is **not** autouse — it only runs when a test explicitly
    depends on it (directly or transitively).  Running
    ``pytest test_lance_spark.py`` alone will never start MinIO.
    """
    import os

    env = os.environ.copy()
    env["MINIO_ROOT_USER"] = MINIO_ROOT_USER
    env["MINIO_ROOT_PASSWORD"] = MINIO_ROOT_PASSWORD

    proc = subprocess.Popen(
        [
            "minio", "server", "/tmp/minio-data",
            "--address", f":{MINIO_PORT}",
            "--console-address", f":{MINIO_CONSOLE_PORT}",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        env=env,
    )

    # Poll until the MinIO health endpoint responds (up to 30 s).
    health_url = f"http://127.0.0.1:{MINIO_PORT}/minio/health/live"
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            urllib.request.urlopen(health_url, timeout=1)
            break
        except (urllib.error.URLError, OSError):
            if proc.poll() is not None:
                raise RuntimeError("minio exited unexpectedly")
            time.sleep(0.5)
    else:
        proc.terminate()
        raise RuntimeError("minio did not become healthy within 30 s")

    # Create the test bucket using boto3.
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url=f"http://127.0.0.1:{MINIO_PORT}",
        aws_access_key_id=MINIO_ROOT_USER,
        aws_secret_access_key=MINIO_ROOT_PASSWORD,
    )
    s3.create_bucket(Bucket=MINIO_BUCKET)

    yield {
        "endpoint": f"http://127.0.0.1:{MINIO_PORT}",
        "access_key": MINIO_ROOT_USER,
        "secret_key": MINIO_ROOT_PASSWORD,
        "bucket": MINIO_BUCKET,
        "port": MINIO_PORT,
    }

    proc.terminate()
    proc.wait(timeout=5)
