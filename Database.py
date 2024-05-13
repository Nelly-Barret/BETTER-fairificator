from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection
import csv
import json
from Utils import *


@decorate_all_functions(check_types_before_func)
class Database:
	def __init__(self, username: str, password: str, cluster: str, app_name: str, server_api: str):
		self.connection_string = "mongodb+srv://"+username+":"+password+"@"+cluster+".qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName="+app_name
		self.client = MongoClient(self.connection_string, server_api=ServerApi(server_api))
		self.db = self.client[cluster]
		print("the connection string is: ")
		print(self.connection_string)
		print("the new MongoClient is: ")
		print(self.client)
		print("the database is: ")
		print(self.db)

	def check_server_is_up(self) -> bool:
		# Send a ping to confirm a successful connection
		try:
		  self.client.admin.command('ping')
		  return True
		except Exception as e:
		  print(e)
		  return False

	def get_table(self, table_name: str) -> Collection: 
		return self.db[table_name]

	def insert_one_tuple(self, table_name, tuple) -> int:
		check_types((type(table_name), str), (type(tuple), dict))

		return self.db[table_name].insert_one(tuple).inserted_id

	def insert_many_tuples(self, table_name: str, tuples: list[dict]) -> list[int]:
		return self.db[table_name].insert_many(tuples).inserted_ids

	def insert_tuples_from_csv(self, table_name: str, csv_path: str, delimiter: str, quotechar: str) -> None:
		print("table_name is: " + table_name)
		print("csv_path is: " + csv_path)
		with open(csv_path, newline='') as csvfile:
			# TODO Nelly: check if we can force DictReader to write double quoted json
			csv_reader = csv.DictReader(csvfile, delimiter=',',quotechar='"', quoting=csv.QUOTE_ALL)
			data = [row for row in csv_reader]
			print(type(data))
			double_quoted_string_data = json.dumps(data)
			print(type(double_quoted_string_data))
			double_quoted_data = json.loads(double_quoted_string_data)
			print(type(double_quoted_data))
			
			# self.insert_many_tuples(table_name, double_quoted_data)

	def __str__(self) -> str:
		return "Database " + self.connection_string