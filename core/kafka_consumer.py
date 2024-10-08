from aiokafka import AIOKafkaConsumer
from settings import settings


class KafkaConsumer:
	def __init__(self, bootstrap_servers, topic):
		self.topic = topic
		self.consumer = AIOKafkaConsumer(
			topic,
			bootstrap_servers=bootstrap_servers,
			group_id=f'{settings.engine}-tac2bufr',
			auto_offset_reset='earliest',
		)

	async def start(self):
		await self.consumer.start()
	async def stop(self):
		await self.consumer.stop()

	async def consume(self):
		try:
			async for msg in self.consumer:
				yield msg.value
		finally:
			await self.consumer.stop()
