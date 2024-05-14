import json

class Hospital:
	def __init__(self, hospital_name: str):
		self.id = 1
		self.hospital_name = hospital_name

	def to_json(self):
		return json.dumps(self)