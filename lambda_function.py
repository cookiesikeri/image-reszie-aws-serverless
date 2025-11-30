import json
import boto3
import os
from datetime import datetime
import uuid

s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')
sns_client = boto3.client('sns')

RESIZED_BUCKET = os.environ.get('RESIZED_BUCKET', '')
DYNAMODB_TABLE = os.environ.get('DYNAMODB_TABLE', 'ImageMetadata')
SNS_TOPIC_ARN = os.environ.get('SNS_TOPIC_ARN', '')
MAX_WIDTH = int(os.environ.get('MAX_WIDTH', '800'))
MAX_HEIGHT = int(os.environ.get('MAX_HEIGHT', '600'))

def lambda_handler(event, context):
    """Process and resize images"""
    
    try:
        # Try to import Pillow
        try:
            from PIL import Image
            from io import BytesIO
            pillow_available = True
            print("✅ Pillow is available - will resize images")
        except ImportError:
            pillow_available = False
            print("⚠️ Pillow not available - will copy images without resizing")
        
        for record in event['Records']:
            source_bucket = record['s3']['bucket']['name']
            object_key = record['s3']['object']['key']
            
            print(f"Processing: {object_key} from {source_bucket}")
            
            # Download image
            response = s3_client.get_object(Bucket=source_bucket, Key=object_key)
            image_content = response['Body'].read()
            original_size = len(image_content)
            content_type = response.get('ContentType', 'image/jpeg')
            
            name, ext = os.path.splitext(object_key)
            new_key = f"{name}_resized{ext}"
            
            # Try to resize if Pillow is available
            if pillow_available:
                try:
                    # Open and resize image
                    img = Image.open(BytesIO(image_content))
                    original_width, original_height = img.size
                    
                    print(f"Original dimensions: {original_width}x{original_height}")
                    
                    # Calculate new dimensions maintaining aspect ratio
                    aspect_ratio = original_width / original_height
                    
                    if original_width > MAX_WIDTH or original_height > MAX_HEIGHT:
                        if aspect_ratio > 1:  # Landscape
                            new_width = MAX_WIDTH
                            new_height = int(MAX_WIDTH / aspect_ratio)
                        else:  # Portrait
                            new_height = MAX_HEIGHT
                            new_width = int(MAX_HEIGHT * aspect_ratio)
                    else:
                        new_width, new_height = original_width, original_height
                    
                    # Resize image
                    resized_img = img.resize((new_width, new_height), Image.LANCZOS)
                    
                    print(f"Resized dimensions: {new_width}x{new_height}")
                    
                    # Save resized image to bytes
                    buffer = BytesIO()
                    img_format = img.format or 'JPEG'
                    resized_img.save(buffer, format=img_format, quality=85, optimize=True)
                    buffer.seek(0)
                    
                    resized_content = buffer.read()
                    resized_size = len(resized_content)
                    
                    # Upload resized image
                    s3_client.put_object(
                        Bucket=RESIZED_BUCKET,
                        Key=new_key,
                        Body=resized_content,
                        ContentType=content_type
                    )
                    
                    print(f"✅ Resized and uploaded to: {RESIZED_BUCKET}/{new_key}")
                    print(f"Size reduction: {original_size/1024:.2f}KB → {resized_size/1024:.2f}KB ({((original_size-resized_size)/original_size*100):.1f}% saved)")
                    
                    dimensions = {
                        'original_width': original_width,
                        'original_height': original_height,
                        'resized_width': new_width,
                        'resized_height': new_height
                    }
                    
                except Exception as resize_error:
                    print(f"⚠️ Resize failed: {str(resize_error)}, copying original instead")
                    # Fall back to copying original
                    s3_client.copy_object(
                        CopySource={'Bucket': source_bucket, 'Key': object_key},
                        Bucket=RESIZED_BUCKET,
                        Key=new_key,
                        ContentType=content_type
                    )
                    resized_size = original_size
                    dimensions = {}
            else:
                # No Pillow - just copy
                s3_client.copy_object(
                    CopySource={'Bucket': source_bucket, 'Key': object_key},
                    Bucket=RESIZED_BUCKET,
                    Key=new_key,
                    ContentType=content_type
                )
                resized_size = original_size
                dimensions = {}
                print(f"Copied to: {RESIZED_BUCKET}/{new_key}")
            
            # Save metadata to DynamoDB
            image_id = str(uuid.uuid4())
            table = dynamodb.Table(DYNAMODB_TABLE)
            
            metadata_item = {
                'image_id': image_id,
                'original_bucket': source_bucket,
                'original_key': object_key,
                'processed_bucket': RESIZED_BUCKET,
                'processed_key': new_key,
                'original_size': original_size,
                'resized_size': resized_size,
                'content_type': content_type,
                'processed_at': datetime.utcnow().isoformat(),
                'status': 'resized' if pillow_available else 'copied',
                'pillow_used': pillow_available
            }
            
            # Add dimensions if available
            if dimensions:
                metadata_item.update(dimensions)
            
            table.put_item(Item=metadata_item)
            print(f"💾 Saved metadata: {image_id}")
            
            # Send notification
            if SNS_TOPIC_ARN:
                if pillow_available and dimensions:
                    size_saved = ((original_size - resized_size) / original_size * 100)
                    message = f"""
✅ Image Resized Successfully!

Original File: {object_key}
Resized File: {new_key}

📏 Dimensions:
  Original: {dimensions['original_width']}x{dimensions['original_height']}
  Resized: {dimensions['resized_width']}x{dimensions['resized_height']}

💾 File Sizes:
  Original: {original_size / 1024:.2f} KB
  Resized: {resized_size / 1024:.2f} KB
  Space Saved: {size_saved:.1f}%

📦 Destination: {RESIZED_BUCKET}
🆔 Image ID: {image_id}
⏰ Processed: {datetime.utcnow().isoformat()}

Your image has been optimized and stored successfully! 🎉
                    """
                else:
                    message = f"""
✅ Image Processed Successfully!

Original File: {object_key}
Processed File: {new_key}
Size: {original_size / 1024:.2f} KB
Bucket: {RESIZED_BUCKET}
Image ID: {image_id}
Time: {datetime.utcnow().isoformat()}

Note: Image was copied without resizing (Pillow not available)
                    """
                
                sns_client.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject='✅ Image Processing Complete',
                    Message=message
                )
                
                print("📧 Notification sent via SNS")
            
            print(f"✅ Successfully processed: {object_key}")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Image processing completed successfully',
                'pillow_available': pillow_available
            })
        }
        
    except Exception as e:
        print(f"❌ Error processing image: {str(e)}")
        
        # Send error notification
        if SNS_TOPIC_ARN:
            try:
                sns_client.publish(
                    TopicArn=SNS_TOPIC_ARN,
                    Subject='❌ Image Processing Failed',
                    Message=f'Error processing image: {str(e)}\nTime: {datetime.utcnow().isoformat()}'
                )
            except Exception as sns_error:
                print(f"Failed to send error notification: {str(sns_error)}")
        
        raise e
