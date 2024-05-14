from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection
import csv
import json
from utils.Utils import *
from utils.setup_logger import log
from bson import SON


@decorate_all_functions(check_types_before_func)
class Database:
	def __init__(self, connection_string="mongodb://localhost:27017/", database_name="my_db"):
		assert type(connection_string) is str 

		# self.connection_string = "mongodb+srv://"+username+":"+password+"@"+cluster+".qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName="+app_name
		self.connection_string = connection_string
		self.client = MongoClient(self.connection_string) #, server_api=ServerApi(server_api))
		self.db = self.client[database_name]
		# result = self.client.admin.command(SON(["getLogComponents"]))
		result = self.client.admin.command(SON([
		    ("setParameter", 1),
		    ("logComponentVerbosity", {
		        "storage": {
		            "verbosity": 5,
		            "journal": {
		                "verbosity": 5
		            }
		        }
		    })]))

		# log.debug(result)
		log.debug("the connection string is: " + str(self.connection_string))
		log.debug("the new MongoClient is: " + str(self.client))
		log.debug("the database is: " + str(self.db))

	def check_server_is_up(self) -> bool:
		# Send a ping to confirm a successful connection
		try:
		  self.client.admin.command('ping')
		  return True
		except Exception as e:
		  loging.error(e)
		  return False

	def get_table(self, table_name: str) -> Collection: 
		assert type(table_name) is str 

		res = self.db[table_name]
		assert type(res) is Collection
		return res

	def create_table(self, table_name: str):
		assert type(table_name) is str 
		self.db[table_name]

	def insert_one_tuple(self, table_name, one_tuple) -> int:
		check_types((type(table_name), str), (type(one_tuple), dict))

		return self.db[table_name].insert_one(tuple).inserted_id

	def insert_many_tuples(self, table_name: str, tuples: list[dict]) -> list[int]:
		return self.db[table_name].insert_many(tuples).inserted_ids

	def insert_tuples_from_csv(self, table_name: str, csv_path: str, delimiter: str, quotechar: str) -> None:
		log.debug("table_name is: " + table_name)
		log.debug("csv_path is: " + csv_path)
		with open(csv_path, newline='') as csvfile:
			# TODO Nelly: check if we can force DictReader to write double quoted json
			csv_reader = csv.DictReader(csvfile, delimiter=',',quotechar='"', quoting=csv.QUOTE_ALL)
			data = [row for row in csv_reader]
			log.debug(type(data))
			double_quoted_string_data = json.dumps(data)
			log.debug(type(double_quoted_string_data))
			double_quoted_data = json.loads(double_quoted_string_data)
			log.debug(type(double_quoted_data))
			
			# self.insert_many_tuples(table_name, double_quoted_data)

	def __str__(self) -> str:
		return "Database " + self.connection_string