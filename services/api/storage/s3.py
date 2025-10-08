import boto3, os

_session = boto3.session.Session(
    aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_SECRET_KEY"),
    region_name="us-east-1",
)

s3 = _session.resource(
    "s3",
    endpoint_url=f"http://{os.getenv('MINIO_ENDPOINT','minio:9000')}",
)

# ─────────────────────────────────────────────────────────────────────────────
# Worker service
# ─────────────────────────────────────────────────────────────────────────────
