class ClinicalRecord:
	def __init__(
					self,
					id: long, 
					instantiates: Examination, 
					value: any, 
					subject: Patient, 
					recorded_by: Hospital, 
					status: str, 
					code: dict, 
					issued: date):
		"""
		Create a new ClinicalRecord instance.

		Parameters:
			id (long): the unique ID to refer to that clinical record in the project.
			instantiates (Examination): the Examination instance that record is refering to.
			value (any): the value of what is examined in that clinical record.
			subject (Patient): the Patient on which the clinical record has been measured.
			recorded_by (Hospital): the Hospital in which the clinical record has been measured.
			status (str): a value in [registered, preliminary, final, amended] depicting the current record status.
			code (str): the Examination url.
			issued (dateTime): when the clinical record has been measured.
		"""
		self.id = id
		self.instantiates = instantiates
		self.value = value
		self.subject = subject
		self.recorded_by = recorded_by
		self.status = status
		self.code = code
		self.issued = issued

	def get_id(self):
		return self.id 

	def get_instantiates(self):
		return self.instantiates

	def get_value(self):
		return self.value

	def get_subject(self):
		return self.subject

	def get_recorded_by(self):
		return self.recorded_by

	def get_status(self):
		return self.status

	def get_code(self):
		return self.code

	def get_issued(self):
		return issued
