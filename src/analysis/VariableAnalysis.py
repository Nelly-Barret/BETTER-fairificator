import json

from src.utils.setup_logger import log
from src.utils.utils import is_not_nan


class VariableAnalysis:
    def __init__(self, samples, metadata):
        self.samples = samples
        self.metadata = metadata

        self.sample_variables = self.samples.columns.to_list()
        self.metadata_variables = self.metadata["name"].to_list()

        # cumulative variables to count what happens in the variable analysis
        self.nb_categorical_features_without_mapping = 0
        self.total_nb_categorical_features = 0
        self.ratio_categorical_feature_with_no_mapping = 0.0
        self.ratio_variable_no_ontology = 0.0

    def run_analysis(self):
        self.__compute_nb_variables_without_ontology()
        self.__compute_nb_categorical_features_without_mapping()

    def __compute_nb_variables_without_ontology(self):
        nb_variables_without_ontology = 0
        for data_variable in self.sample_variables:
            if data_variable not in self.metadata_variables:
                nb_variables_without_ontology += 1
        total_number_variables = len(self.sample_variables)
        self.ratio_variable_no_ontology = nb_variables_without_ontology / total_number_variables
        log.debug("Number of variables without ontology: %s/%s=%s", nb_variables_without_ontology, total_number_variables, self.ratio_variable_no_ontology)

    def __compute_nb_categorical_features_without_mapping(self):
        self.nb_categorical_features_without_mapping = 0
        for index, metadata_variable in self.metadata.iterrows():
            if metadata_variable["vartype"] == "category":
                if not is_not_nan(metadata_variable["JSON_values"]):
                    log.debug(metadata_variable["name"])
                    self.nb_categorical_features_without_mapping += 1
                self.total_nb_categorical_features += 1
        self.ratio_categorical_feature_with_no_mapping = self.nb_categorical_features_without_mapping / self.total_nb_categorical_features
        log.debug("Ratio of categorical feature having no mapping: %s/%s=%s", self.nb_categorical_features_without_mapping, self.total_nb_categorical_features, self.ratio_categorical_feature_with_no_mapping)

    def to_json(self):
        return {
            "nb_categorical_features_without_mapping": str(self.nb_categorical_features_without_mapping),
            "total_nb_categorical_features": str(self.total_nb_categorical_features),
            "ratio_categorical_feature_with_no_mapping": str(self.ratio_categorical_feature_with_no_mapping),
            "ratio_variable_no_ontology": str(self.ratio_variable_no_ontology)
        }

    def __repr__(self):
        return json.dumps(self.to_json())