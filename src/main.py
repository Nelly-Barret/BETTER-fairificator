from database.Database import *
from etl.ETL import *
from utils.setup_logger import log

def test_mongo():	
	# set a new connection to the locally running MongoDB
	database = Database()
	connection_up = database.check_server_is_up()
	if(connection_up):
		log.info("The connection is up.")
	else:
		log.error("There was a problem while connecting to the MongoDB server.")
		exit()

	# mongodb+srv://nellybarret:aeyuZpyUOJGI0PpS@better-cluster.qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName=BETTER-application
	# mongodb+srv://nellybarret:<password>@better-cluster.qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName=BETTER-application

	# create a table, called a collection in the MongoDB language
	# patients_table = database.get_table("patients") 

	# database.insert_tuples_from_csv("patients2", "../data/buzzi_subset_small_quoted.csv", ',', '')

	# create a first patient and fill its data
	# first_patient = { "first-name": "Bob", "last-name": "Doe", "condition": "breast cancer" }

	# log.debug("my first patient: ")
	# log.debug(first_patient)

	# actually insert the first patient in the database
	# this will also create the collection (as soon as the first tuple is inserted)
	# first_patient_id = patients_table.insert_one(first_patient).inserted_id

	# log.debug("First patient has been inserted. Its ID is: " + str(first_patient_id))

	# # get one tuple of the patients table
	# log.debug(patients_table.find_one())

	# # print the first patient having for first-name "Alice"
	# log.debug(patients_table.find_one({"first-name": "Alice"}))

if __name__ == '__main__':

	log.info("Hello world!")

	test_mongo()

	# my_metadata_filepath = "/Users/nelly/Documents/boulot/postdoc-polimi/BETTER-fairificator/data/metadata/IT-Buzzi-variables.csv"
	# my_samples_filepath = "/Users/nelly/Documents/boulot/postdoc-polimi/BETTER-fairificator/data/samples/BUZZI/buzzi_subset_small_quoted.csv"
	# etl = ETL(my_metadata_filepath, my_samples_filepath)
	# etl.run()

	log.info("Goodbye world!")