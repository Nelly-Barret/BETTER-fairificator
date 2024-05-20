import os

import pandas as pd
import re

from src.analysis.ValueAnalysis import ValueAnalysis
from src.analysis.VariableAnalysis import VariableAnalysis
from src.database.Database import *
from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.profiles.Examination import Examination
from src.profiles.ExaminationRecord import ExaminationRecord
from src.profiles.Hospital import Hospital
from src.profiles.Patient import Patient
from src.analysis.DistributionPlot import DistributionPlot
from src.utils.utils import EXAMINATION_TABLE_NAME, EXAMINATION_RECORD_TABLE_NAME, HOSPITAL_TABLE_NAME, \
    PATIENT_TABLE_NAME, CATEGORY_CLINICAL, CATEGORY_PHENOTYPIC, normalize_value, is_not_empty, \
    get_values_from_JSON_values, ONTOLOGY_SNOMED_NAME, convert_value, ONTOLOGY_LOINC_NAME, is_equal_insensitive
from src.utils.utils import build_url, is_not_nan, get_ontology_system, get_ontology_code
from src.utils.setup_logger import log


class ETL:
    def __init__(self, hospital_name, metadata_filepath: str, samples_filepath: str, reset_db: bool):
        self.database = Database(connection_string="mongodb://localhost:27017/", database_name="better_database_mini")
        if reset_db:
            self.database.reset()
        self.create_structures = True
        self.run_analysis = True
        self.transform_data = False
        self.load_data = False
        self.compute_plots = False
        # self.database = Database()
        self.hospital_name = hospital_name
        self.hospital = self.__create_hospital()
        self.metadata_filepath = metadata_filepath
        self.samples_filepath = samples_filepath
        self.metadata = None
        self.patients = []
        self.examinations = []
        self.examination_records = []
        self.phenotypic_variables = {}
        self.mapping_colname_to_examination = {}
        self.mapped_values = {}  # accepted values for some categorical columns (column "JSON_values" in metadata)
        self.mapped_types = {}  # expected data type for columns (column "vartype" in metadata)

    def run(self):
        # self.__create_db_indexes()

        # 0. set filepaths as arguments
        # 1. set db name: better_project
        # 2. uncomment the three lines below
        if self.create_structures:
            self.__load_metadata_file()
            self.__load_samples_file()
            self.__load_phenotypic_variables()
            self.__compute_mapped_values()
            self.__compute_mapped_types()

        if self.run_analysis:
            # self.__run_value_analysis()
            self.__run_variable_analysis()

        if self.transform_data:
            self.__transform_data()

        if self.load_data:
            self.__insert_data()

        # #2. MongoDB compass
        # # Show the entry for Hospital
        # # Show first Patient
        # # Show structure of one Examination
        # # all examination records for patient 1: {"subject.reference": "Patient1"}
        # # examination "Ethnicity" for Patient 1: {"$and": [{"subject.reference": "Patient1"}, {"instantiate.reference": "77"}]}
        #
        # # 3. comment the 3 lines above
        # # uncomment lines below
        if self.compute_plots:
            examination_url = build_url(EXAMINATION_TABLE_NAME, 88)  # premature baby
            cursor = self.database.get_value_distribution_of_examination(EXAMINATION_RECORD_TABLE_NAME, examination_url, -1)
            plot = DistributionPlot(cursor, examination_url, "Premature Baby", False)  # do not print the cursor before, otherwise it would consume it
            plot.draw()

            examination_url = build_url(EXAMINATION_TABLE_NAME, 77)  # ethnicity
            cursor = self.database.get_value_distribution_of_examination(EXAMINATION_RECORD_TABLE_NAME, examination_url,  20)
            plot = DistributionPlot(cursor, examination_url, "Ethnicity", True)  # do not print the cursor before, otherwise it would consume it
            plot.draw()

        # cursor = self.database.get_min_value_of_examination_record(TableNames.EXAMINATION_RECORD_TABLE_NAME, "76")
        # cursor = self.database.get_avg_value_of_examination_record(TableNames.EXAMINATION_RECORD_TABLE_NAME, "76")

    def __create_db_indexes(self):
        self.database.create_unique_index(table_name=PATIENT_TABLE_NAME, columns={"metadata.csv_filepath": 1, "metadata.csv_line": 1})
        self.database.create_unique_index(table_name=HOSPITAL_TABLE_NAME, columns={"id": 1, "name": 1})
        self.database.create_unique_index(table_name=EXAMINATION_TABLE_NAME, columns={"code.codings.system": 1, "code.codings.code": 1})
        self.database.create_unique_index(table_name=EXAMINATION_RECORD_TABLE_NAME, columns={"instantiate.reference": 1, "subject.reference": 1, "recorded_by.reference": 1, "value": 1, "issued": 1})

    def __load_phenotypic_variables(self):
        self.phenotypic_variables = json.load(open(os.path.join("data", "metadata", "phenotypic-variables.json")))
        log.debug("Nb of phenotypic variables: %s", len(self.phenotypic_variables))

    def __compute_mapped_values(self):
        self.mapped_values = {}

        for index, row in self.metadata.iterrows():
            if is_not_nan(row["JSON_values"]):
                current_dicts = json.loads(row["JSON_values"])
                parsed_dicts = []
                for current_dict in current_dicts:
                    # if we can convert the JSON value to a float or an int, we do it, otherwise we let it as a string
                    current_dict["value"] = convert_value(current_dict["value"])
                    # if we can also convert the snomed_ct / loinc code, we do it
                    if ONTOLOGY_SNOMED_NAME in current_dict:
                        current_dict[ONTOLOGY_SNOMED_NAME] = convert_value(current_dict[ONTOLOGY_SNOMED_NAME])
                    if ONTOLOGY_LOINC_NAME in current_dict:
                        current_dict[ONTOLOGY_LOINC_NAME] = convert_value(current_dict[ONTOLOGY_LOINC_NAME])
                    parsed_dicts.append(current_dict)
                self.mapped_values[row["name"]] = parsed_dicts
        log.debug(self.mapped_values)

    def __compute_mapped_types(self):
        self.mapped_types = {}

        for index, row in self.metadata.iterrows():
            if is_not_nan(row["vartype"]):
                self.mapped_types[row["name"]] = row["vartype"]  # we associate the column name to its expected type
        log.debug(self.mapped_types)

    def __create_hospital(self):
        new_hospital = Hospital(self.hospital_name)
        hospital_tuple = new_hospital.to_json()

        # check if this hospital has already been created
        # if so, do not create it again, and get its OD
        # otherwise, create it with a new ID
        filter_hospital_name = {}
        # filter_hospital_name = {
        #     "name": new_hospital.get_name()
        # }
        nb_hospitals_with_name = self.database.count_documents(HOSPITAL_TABLE_NAME, filter_hospital_name)
        log.debug("Nb of hospitals with name '%s': %d", self.hospital_name, nb_hospitals_with_name)
        if nb_hospitals_with_name == 0:
            # the hospital does not exist yet, we will create it
            log.info("No hospital labelled '%s' exists: creating it.", self.hospital_name)
            self.database.insert_one_tuple(HOSPITAL_TABLE_NAME, hospital_tuple)
            return new_hospital
        elif nb_hospitals_with_name == 1:
            # the hospital already exists, no need to do something
            log.info("One hospital labelled '%s' exists: retrieving it.", self.hospital_name)
            return new_hospital # TODO: return the hospital from the database
        else:
            raise ValueError("Several hospitals are labelled '%s': stopping here to avoid further problems.", new_hospital.get_hospital_name())

    def __transform_data(self):
        self.__create_examinations()
        self.__create_fair_samples()

    def __insert_data(self):
        self.database.insert_many_tuples(PATIENT_TABLE_NAME, [patient.to_json() for patient in self.patients])
        self.database.insert_many_tuples(EXAMINATION_TABLE_NAME, [examination.to_json() for examination in self.examinations])
        self.database.insert_many_tuples(EXAMINATION_RECORD_TABLE_NAME, [examination_record.to_json() for examination_record in self.examination_records])

    def __load_metadata_file(self):
        assert os.path.exists(self.metadata_filepath), "The provided metadata file could not be found. Please check the filepath you specify when running this script."

        # index_col is False to not add a column with line numbers
        self.metadata = pd.read_csv(self.metadata_filepath, index_col=False)

        # lower case all column names to avoid inconsistencies
        self.metadata['name'] = self.metadata['name'].apply(lambda x: x.lower())

        # remove spaces in ontology names and codes
        self.metadata["ontology"] = self.metadata["ontology"].apply(lambda value: str(value).replace(" ", ""))
        self.metadata["ontology_code"] = self.metadata["ontology_code"].apply(lambda value: str(value).replace(" ", ""))
        self.metadata["secondary_ontology"] = self.metadata["secondary_ontology"].apply(lambda value: str(value).replace(" ", ""))
        self.metadata["secondary_ontology_code"] = self.metadata["secondary_ontology_code"].apply(lambda value: str(value).replace(" ", ""))

        # the non-NaN JSON_values values are of the form: "{...}, {...}, ..."
        # thus, we need to
        # 1. create an empty list
        # 2. add each JSON dict of JSON_values in that list
        # Note: we cannot simply add brackets around the dicts because it would add a string with the dicts in the list
        # we cannot either use json.loads on row["JSON_values"] because it is not parsable (it lacks the brackets)
        for index, row in self.metadata.iterrows():
            if is_not_nan(row["JSON_values"]):
                values_dicts = []
                json_dicts = re.split('}, {', row["JSON_values"])
                for json_dict in json_dicts:
                    if not json_dict.startswith("{"):
                        json_dict = "{" + json_dict
                    if not json_dict.endswith("}"):
                        json_dict = json_dict + "}"
                    values_dicts.append(json.loads(json_dict))
                self.metadata.loc[index, "JSON_values"] = json.dumps(values_dicts) # set the new JSON values as a string

    def __load_samples_file(self):
        assert os.path.exists(self.samples_filepath), "The provided samples file could not be found. Please check the filepath you specify when running this script."

        log.debug("self.samples_filepath is %s", self.samples_filepath)

        # index_col is False to not add a column with line numbers
        self.samples = pd.read_csv(self.samples_filepath, index_col=False)

        # lower case all column names to avoid inconsistencies
        self.samples.columns = self.samples.columns.str.lower()

    def __create_fair_samples(self):
        for index, sample in self.samples.iterrows():
            # create patients, one per sample, and add it to self.patients
            # TODO Pietro: one patient per sample for now, but it is not true. How do we know the list of patients?
            patient = self.__create_patient(sample)
            # create clinical and phenotypic records by associating observations to patients
            for column_name, value in sample.items():
                if value is None or value == "" or not is_not_nan(value):
                    # there is no value for that examination for the current patient
                    # therefore we do not create an examination record and skip this cell
                    pass
                else:
                    if column_name in self.mapping_colname_to_examination:
                        # log.info("I know a code for column %s", column_name)
                        # we know a code for this column, so we can register the value of that examination
                        associated_examination = self.mapping_colname_to_examination[column_name]
                        # TODO Pietro: harmonize values (upper-lower case, dates, etc)
                        #  we should use codes (JSON-styled by Boris) whenever we can
                        #  but for others we need normalization (WP6?)
                        #  we could even clean more the data, e.g., do not allow "Italy" as ethnicity (caucasian, etc)
                        fairified_value = self.__fairify_value(column_name=column_name, value=value)
                        # log.info("I'm registering value %s for column %s from value %s", fairified_value, column_name, value)
                        self.create_examination_record(self.hospital, patient, associated_examination, fairified_value)
                    else:
                        # Ideally, there will be no column left without a code
                        # So this should never happen
                        pass
        # TODO Nelly: add unique constraints to avoid duplicates
        log.debug("Nb of patients: %s", len(self.patients))
        log.debug("Nb of examination records: %s", len(self.examination_records))

    def __create_patient(self, sample):
        # log.debug(sample)
        new_patient = Patient(sample["line"], self.samples_filepath)
        self.patients.append(new_patient)
        return new_patient

    def create_examination_record(self, hospital: Hospital, patient: Patient, associated_examination: Examination, value):
        status = ""
        code = ""
        issued = ""
        interpretation = ""
        self.examination_records.append(ExaminationRecord(associated_examination, status, code, patient, hospital, value, issued, interpretation))

    def __create_examinations(self):
        columns = self.samples.columns.values.tolist()
        for column in columns:
            code = self.__determine_code_from_metadata(column)
            if code is not None and code.codings != []:
                status = "registered"
                category = self.__determine_examination_category(code)
                permitted_datatypes = []
                multiple_results_allowed = False
                body_site = ""
                new_examination = Examination(code, status, category, permitted_datatypes, multiple_results_allowed, body_site)
                self.examinations.append(new_examination)
                self.mapping_colname_to_examination[column] = new_examination
            else:
                # This should never happen as all variables will end up to have a code
                pass

        log.debug("Nb of examinations: %s", len(self.examinations))

    def __determine_code_from_metadata(self, column_name: str):
        rows = self.metadata.loc[self.metadata['name'] == column_name]
        if len(rows) == 1:
            cc = CodeableConcept()
            cc_tuple = self.create_code_from_metadata(rows, "ontology", "ontology_code")
            if cc_tuple is not None:
                cc.add_coding(cc_tuple)
            cc_tuple = self.create_code_from_metadata(rows, "secondary_ontology", "secondary_ontology_code")
            if cc_tuple is not None:
                cc.add_coding(cc_tuple)
            return cc
        elif len(rows) == 0:
            # log.warn("Did not find the column '%s' in the metadata", column_name)
            return None
        else:
            # log.warn("Found several times the column '%s' in the metadata", column_name)
            return None

    def create_code_from_metadata(self, rows, ontology_column: str, code_column: str):
        ontology = rows[ontology_column].iloc[0]
        if is_not_nan(ontology):
            # no ontology code has been provided for that variable name, let's skip it
            return None
        else:
            code = rows[code_column].iloc[0]
            display = rows["name"].iloc[0]
            if is_not_nan(rows["description"].iloc[0]):
                # by default the display is the variable name
                # if we also have a description, we append it to the display
                # e.g., "BTD. human biotinidase activity."
                display = display + ". " + str(rows["description"].iloc[0]) + "."
            # log.info("Found exactly an ontology code for the column '%s', i.e., %s", column_name, code)
            cc_tuple = (str(ontology), str(code), str(display))
            return cc_tuple

    def __determine_examination_category(self, code: CodeableConcept):
        for coding in code.codings:
            coding_full_name = coding.system + "/" + coding.code
            if coding_full_name in self.phenotypic_variables:
                return CodeableConcept(CATEGORY_PHENOTYPIC)  #  we can't create the CC directly in utils due to circular imoprts
            else:
                return CodeableConcept(CATEGORY_CLINICAL)

    def __fairify_value(self, column_name, value):
        if column_name in self.mapped_values:
            # we iterate over all the mappings of a given column
            for mapping in self.mapped_values[column_name]:
                # we get the value of the mapping, e.g., F, or M, or NA
                mapped_value = mapping['value']
                # if the sample value is equal to the mapping value,
                # we have found a match and we will record the associated ontology term instead of the value
                if is_equal_insensitive(value, mapped_value):
                    # we create a CodeableConcept with each code added to the mapping, e.g., snomed_ct and loinc
                    # recall that a mapping is of the form: {'value': 'X', 'explanation': '...', 'snomed_ct': '123', 'loinc': '456' }
                    # and we add each ontology code to that CodeableConcept
                    cc = CodeableConcept()
                    for key, val in mapping.items():
                        # for any key value pair that is not about the value or the explanation
                        # (i.e., loinc and snomed_ct columns), we create a Coding, which we add to the CodeableConcept
                        # we need to do a loopp because there may be several ontology terms for a single mapping
                        if key != 'value' and key != 'explanation':
                            system = get_ontology_system(key)
                            code = get_ontology_code(val)
                            display = mapping['explanation']
                            cc.add_coding((system, code, display))
                    return cc  # return the CC computed out of the corresponding mapping
            return normalize_value(value)  # no coded value for that value, trying at least to normalize it a bit
        return normalize_value(value)  # no coded value for that value, trying at least to normalize it a bit

    def __run_value_analysis(self):
        log.debug(self.mapped_values)
        # for each column in the sample data (and not in the metadata because some (empty) data columns are not
        # present in the metadata file), we compare the set of values it takes against the accepted set of values
        # (available in the mapped_values variable)
        for column in self.samples.columns:
            values = pd.Series(self.samples[column].values)
            values = values.apply(lambda value: value.casefold().strip() if isinstance(value, str) else value)
            # log.debug("Values are: %s", values)
            # log.debug(self.metadata["name"])
            log.info("Working on column '%s'", column)
            # trying to get the expected type of the current column
            if column in self.mapped_types:
                expected_type = self.mapped_types[column]
            else:
                expected_type = ""
            # trying to get expected values for the current column
            if column in self.mapped_values:
                # self.mapped_values[column] contains the mappings (JSON dicts) for the given column
                # we need to get only the set of values described in the mappings of the given column
                accepted_values = get_values_from_JSON_values(json_values=self.mapped_values[column])
            else:
                accepted_values = []
            analysis = ValueAnalysis(column_name=column, values=values, expected_type=expected_type, accepted_values=accepted_values)
            analysis.run_analysis()

    def __run_variable_analysis(self):
        variable_analysis = VariableAnalysis(samples=self.samples, metadata=self.metadata)
        variable_analysis.run_analysis()
