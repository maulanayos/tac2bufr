from pydantic_settings import BaseSettings


class Settings(BaseSettings):
	# env: str = "dev"
	engine: str
	# DB
	# database_url: str

	# REDIS
	redis_url: str

	# database URI
	database_url: str

	# MINIO
	minio_url: str
	minio_access_key: str
	minio_secret_key: str
	minio_incoming_bucket: str

	# REDPANDA
	redpanda_brokers: str
	redpanda_incoming_topic: str
	redpanda_serialized_topic: str

	class Config:
		env_file = '.env'


settings = Settings()
