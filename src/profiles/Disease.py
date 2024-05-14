class Disease:
	def __init__(
					self, 
					url: str,
					status: str, 
					code: CodeableConcept)
		"""
		Create a new Disease instance. 
		This is different from a DiseaseRecord: 
		- a Disease instance models a disease definition
		- a DiseaseRecord instance models that Patient P has Disease D

		Parameters:
			url (str) : the (unique) uri to identify this disease in the project. 
			status (str): a value in [draft, active, retired, unknown] to specify whether this disease definition can still be used.
			code (CodeableConcept): the set of ontology terms (LOINC, ICD, ...) refering to that disease.

		"""
		self.url = url
		self.status = status
		self.code = code