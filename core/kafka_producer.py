from aiokafka import AIOKafkaProducer
import json


class KafkaProducer:
	def __init__(self, bootstrap_servers):
		self.producer = AIOKafkaProducer(bootstrap_servers=bootstrap_servers, client_id='bufr-converter')

	async def start(self):
		await self.producer.start()

	async def stop(self):
		await self.producer.stop()

	async def send_message(self, topic, message):
		try:
			value = json.dumps(message).encode()
			await self.producer.send_and_wait(topic, value)
		except Exception as e:
			print(f'Error sending message: {e}')
