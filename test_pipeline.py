import boto3
import time
import uuid

def test_image_pipeline():
    """Test complete image processing pipeline"""
    
    s3 = boto3.client('s3')
    dynamodb = boto3.resource('dynamodb')
    
    # Upload test image
    test_key = f'test-{uuid.uuid4()}.jpg'
    source_bucket = 'image-source-yourname-2025'
    dest_bucket = 'image-resized-yourname-2025'
    
    with open('test-image.jpg', 'rb') as f:
        s3.put_object(Bucket=source_bucket, Key=test_key, Body=f)
    
    print(f"✅ Uploaded: {test_key}")
    
    # Wait for processing
    time.sleep(10)
    
    # Check resized image exists
    resized_key = test_key.replace('.jpg', '_resized.jpg')
    try:
        s3.head_object(Bucket=dest_bucket, Key=resized_key)
        print(f"✅ Resized image exists: {resized_key}")
    except:
        print(f"❌ Resized image not found")
        return False
    
    # Check DynamoDB record
    table = dynamodb.Table('ImageMetadata')
    response = table.scan(
        FilterExpression='original_key = :key',
        ExpressionAttributeValues={':key': test_key}
    )
    
    if response['Items']:
        print(f"✅ Metadata saved in DynamoDB")
        return True
    else:
        print(f"❌ Metadata not found")
        return False

# Run test
if __name__ == '__main__':
    test_image_pipeline()
