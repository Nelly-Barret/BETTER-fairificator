import pandas as pd
import os

class ETL:
	def __init__(self, metadata_filepath: str, samples_filepath: str):
		self.metadata_filepath = metadata_filepath
		self.samples_filepath = samples_filepath
		self.database = Database()

	def run(self):
		self.__create_tables()
		self.__create_hospital()
		# self.__load_metadata_file()
		# self.__load_samples_file()


	def __load_metadata_file(self):
		assert os.path.exists(self.metadata_filepath)

		log.debug("self.metadata_filepath is " + self.metadata_filepath)

		self.metadata = pd.read_csv(self.metadata_filepath)

		log.debug(self.metadata[0:10])
		
	def __load_samples_file(self):
		assert os.path.exists(self.samples_filepath)
		
		log.debug("self.samples_filepath is " + self.samples_filepath)

		self.samples = pd.read_csv(self.samples_filepath)

		log.debug(self.samples[0:10])

	def __generate_mapping_using_metadata(self):
		self.mapping_columns_to_ontologies = {}

	def __create_tables(self):
		self.database.create_table("Hospital")
		self.database.create_table("Patient")
		self.database.create_table("Examination")
		self.database.create_table("ClinicalRecord")
		self.database.create_table("PhenotypicRecord")

		self.database.list_collection_names()

	def __create_hospital(self, hospital_name: str):
		new_hospital = Hospital(hospital_name)
		hospital_tuple = new_hospital.to_json()
		self.database.insert_one_tuple("Hospital", hospital_tuple)
		
	def __insert_patients_from_samples(self):
		self.database.insert_many_tuples()