import boto3
import concurrent.futures
from pathlib import Path

s3 = boto3.client('s3')
bucket = 'image-source-yourname-2025'

def upload_image(index):
    """Upload single test image"""
    with open('test-image.jpg', 'rb') as f:
        s3.put_object(
            Bucket=bucket,
            Key=f'test-{index}.jpg',
            Body=f
        )
    return f"Uploaded test-{index}.jpg"

# Upload 100 images concurrently
with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(upload_image, range(100))

print(list(results))
