from datatypes.Reference import Reference
from profiles.Analysis import Analysis
from profiles.Resource import Resource
from utils.TableNames import TableNames


class GenomicData(Resource):
    def __init__(self, id_value: str, analysis: Analysis, subject_ref: Reference, hospital_ref: Reference):
        super().__init__(id_value=id_value, resource_type=self.get_type())

        self.analysis = analysis
        self.subject = subject_ref
        self.recorded_by = hospital_ref

    def get_type(self):
        return TableNames.GENOMIC_DATA.value

    def to_json(self):
        return {
            "identifier": self.identifier,
            "resourceType": self.get_type(),
            "analysis": self.analysis.to_json(),
            "subject": self.subject.to_json(),
            "recordedBy": self.recorded_by.to_json()
        }