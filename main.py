#tambah yteyeye
import asyncio
import json
import logging
from typing import Callable
from core.kafka_consumer import KafkaConsumer
from core.kafka_producer import KafkaProducer
from functools import partial
from minio import Minio 
from engine.utils import send_to_kafka
from models.model import PayloadOutModel
from settings import settings
from engine.converter import synop2bufr, pilot2bufr, temp2bufr
from engine.utils import put_data_to_minio
from shared_tools.logging_utils import AsyncExternalKafkaHandler

# import managers
from core.metadata_list_manager import MetadataManager

#setup logger
# logger = setup_logger(
# 	service_name='bufr_converter',
# 	bootstrap_servers=[settings.redpanda_brokers],
# )
incoming_bucket = settings.minio_incoming_bucket

logger = logging.getLogger('bufr-converter')
class BufrConverter:
    def __init__(self, minio_client: Minio, kafka_producer: KafkaProducer, metadata_manager: MetadataManager):
        self.minio_client = minio_client
        self.kafka_producer = kafka_producer.producer
        self.metadata_manager = metadata_manager
    
    async def convert(self,payload_in, converter:Callable):
        default_extra = {
            'ciid': payload_in['circuit_in_id'],
            'context': {'file_id': payload_in['file_id']},
        }

        try:
            metadata = self.metadata_manager.get_data()
            payload = await converter(payload_in, metadata)
            logger.debug('Converted to BUFR', extra={**default_extra, 'step': 'message_converted'})
            return payload
        except Exception as e:
            logger.error(f'Error converting to Bufr: {str(e)}', extra={**default_extra, 'step': 'bufr_converter'})
            return None

    
async def converter_select(processor: BufrConverter, raw: bytes):
    try:
        raw_str = raw.decode('utf-8')
        payload_in = json.loads(raw_str)
        ttaa= payload_in['tt'] + payload_in['aa']
        ttaaii = ttaa + payload_in['ii']
        mapper = processor.metadata_manager.get_mapper()
        default_extra = {
            'ciid': payload_in['circuit_in_id'],
            'ttaaii' : ttaaii,
            'file_id': payload_in['file_id'],
        }

        mapper_check = [d for d in mapper if d.get("ttaaii_in") == ttaaii]
        if not mapper_check:
            logger.error(f"TTAAII {ttaaii} not found in mapper", extra={**default_extra, 'step': 'converter_select', 'error': 'TTAAII not found in mapper'})
            raise ValueError(f"TTAAII {ttaaii} not found in mapper")

        output_ttaii = mapper_check[0]['ttaaii_out']
        
        if ttaa == 'SMID'or ttaa == 'SNID'or ttaa== 'SIID':
            m,processed_data = await processor.convert(payload_in,synop2bufr)
        elif ttaa == 'UPID' or ttaa == 'UGID' or ttaa == 'UHID' or ttaa == 'UQID':
            m,processed_data = await processor.convert(payload_in,pilot2bufr)
        elif ttaa == 'USID' or ttaa == 'ULID' or ttaa == 'UKID' or ttaa == 'UEID' :
            m,processed_data = await processor.convert(payload_in,temp2bufr)
        else:
            processed_data = None
        if processed_data:
            payload_out= PayloadOutModel(**payload_in)
            payload_out.tt = output_ttaii[0:2]
            payload_out.aa = output_ttaii[2:4]
            payload_out.ii = output_ttaii[4:6]
            payload_out.header = output_ttaii + ' WIIX' + payload_out.header[11:]
            payload_out.message = None
            payload_out.type = 'BUFR'
            payload_out.source = "tac2bufr"
            payload_out.filling_time = payload_out.filling_time.strftime('%Y-%m-%dT%H:%M:00.000000Z')
            payload_out.timestamp = payload_out.timestamp.strftime('%Y-%m-%dT%H:%M:00.000000Z')
            processed_data = (payload_out.header+'\n\n').encode('utf-8') + processed_data
            try :
                await put_data_to_minio(processor.minio_client, payload_in['file_id'], processed_data)
                await send_to_kafka(processor.kafka_producer, vars(payload_out))
                logger.info('Data processed and produced', extra={**default_extra, 'step': 'bufr_export'})
            except Exception as e:
                logger.error(f'Error sending data: {str(e)}', extra={**default_extra, 'step': 'storing_bufr', 'error': 'Error storing data to Kafka/minio'})
        elif m:
            logger.warning('No data converted', extra={**default_extra, 'step': 'data_converting', 'error': m})
        else :
            pass
    except Exception as e:
        logger.warning(f'Error processing raw: {str(e)}', extra={'step': 'process_raw'})

async def main():
    minio_client = None
    kafka_producer = None
    kafka_consumer = None
    update_task = None

    try:
        metadata_manager = MetadataManager(settings.database_url)
        await metadata_manager.ensure_updated()

        update_task = asyncio.create_task(metadata_manager.run_update())

        kafka_consumer = KafkaConsumer(settings.redpanda_brokers, settings.redpanda_incoming_topic)
        kafka_producer = KafkaProducer(settings.redpanda_brokers)
        # start the producer immidiately
        await kafka_producer.start()

        minio_client = Minio(
            settings.minio_url,
            access_key=settings.minio_access_key,
            secret_key=settings.minio_secret_key,
            secure=False,
        )

        logger.setLevel(logging.INFO)
        handler = AsyncExternalKafkaHandler(kafka_producer.producer,"logs")
        logger.addHandler(handler)

        logger.info('Kafka consumer and producer started successfully')

        processor = BufrConverter(minio_client, kafka_producer, metadata_manager)
        process_func = partial(converter_select, processor)

        await kafka_consumer.start()
        async for raw in kafka_consumer.consume():
            await process_func(raw)

    except Exception as e:
        logger.error(f'Fatal error: {str(e)}', exc_info=True)
    finally:
        logger.info('Shutting down Kafka consumer and producer')
        if kafka_consumer:
            await kafka_consumer.stop()
        if kafka_producer:
            await kafka_producer.stop()
        if update_task:
            update_task.cancel()
            await update_task
            await metadata_manager.close()


if __name__ == '__main__':
	asyncio.run(main())

