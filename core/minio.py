from minio import Minio


class S3Client:
	def __init__(self, endpoint, access_key, secret_key):
		self.client = Minio(
			endpoint, access_key=access_key, secret_key=secret_key, secure=True
		)

	def get_object(self, bucket_name, object_name):
		return self.client.get_object(bucket_name, object_name)

	def put_object(self, bucket_name, object_name, data, length):
		return self.client.put_object(bucket_name, object_name, data, length)
