from enum import Enum


class TableNames(Enum):
    HOSPITAL = "Hospital"
    PATIENT = "Patient"
    EXAMINATION = "Examination"
    EXAMINATION_RECORD = "ExaminationRecord"
    DISEASE_RECORD = "DiseaseRecord"
    DISEASE = "Disease"
    SAMPLE = "Sample"
    GENOMIC_DATA = "GenomicData"
    MEDICINE = "Medicine"
    MEDICINE_RECORD = "MedicineRecord"
    EXECUTION = "Execution"
