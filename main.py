from Database import *

if __name__ == '__main__':
	
	# set a new connection to the MongoDB client and connect to the server
	database = Database("nellybarret", "aeyuZpyUOJGI0PpS", "BETTER-cluster", "BETTER-application", '1')
	print(database)
	connection_up = database.check_server_is_up()
	if(connection_up):
		print("The connection is up.")
	else:
		print("There was a problem while connecting to the MongoDB server.")
		exit()

	# mongodb+srv://nellybarret:aeyuZpyUOJGI0PpS@better-cluster.qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName=BETTER-application
	# mongodb+srv://nellybarret:<password>@better-cluster.qo5xs5j.mongodb.net/?retryWrites=true&w=majority&appName=BETTER-application

	# create a table, called a collection in the MongoDB language
	# patients_table = database.get_table("patients") 

	database.insert_tuples_from_csv("patients2", "../data/buzzi_subset_small_quoted.csv", ',', '')

	# create a first patient and fill its data
	# first_patient = { "first-name": "Bob", "last-name": "Doe", "condition": "breast cancer" }

	# print("my first patient: ")
	# print(first_patient)

	# actually insert the first patient in the database
	# this will also create the collection (as soon as the first tuple is inserted)
	# first_patient_id = patients_table.insert_one(first_patient).inserted_id

	# print("First patient has been inserted. Its ID is: " + str(first_patient_id))

	# # get one tuple of the patients table
	# print(patients_table.find_one())

	# # print the first patient having for first-name "Alice"
	# print(patients_table.find_one({"first-name": "Alice"}))
