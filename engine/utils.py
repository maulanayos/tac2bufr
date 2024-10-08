import io
from settings import settings
import logging
from minio import Minio
from aiokafka import AIOKafkaProducer
from typing import Optional
import json

logger = logging.getLogger(__name__)


async def retrieve_data_from_minio(
	minio_client: Minio, file_id: str
) -> Optional[bytes]:
	try:
		response = minio_client.get_object(settings.minio_incoming_bucket, file_id)
		return response.read()
	except Exception as e:
		logger.error(f'Error retrieving object from Minio: {e}')
		return None

async def put_data_to_minio(
    minio_client: Minio, 
    object_name: str, 
    data: bytes, 
    bucket_name: str = settings.minio_incoming_bucket, 
    content_type: str = 'application/octet-stream',
	extra = None
) -> bool:
    try:
        data_stream = io.BytesIO(data)
        minio_client.put_object(
            bucket_name,
            object_name,
            data_stream,
            length=len(data),
            content_type=content_type
        )
        logger.info(f'Successfully uploaded object {object_name} to bucket {bucket_name}')
        return True
    except Exception as e:
        logger.error(f'Error uploading object to Minio: {e}', extra={**extra} if extra else {})
        return False
	
	
async def send_to_kafka(kafka_producer: AIOKafkaProducer, model) -> None:
	try:
		await kafka_producer.send_and_wait(
			'serialized', json.dumps(model, default=str).encode('utf-8')
		)
	except Exception as e:
		logger.error(f'Error sending messages to Kafka: {e}', exc_info=True)


