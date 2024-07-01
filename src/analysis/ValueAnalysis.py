import json

from pandas import Series

from utils.utils import get_int_from_str, is_not_nan, get_float_from_str, is_equal_insensitive, is_not_empty, \
    get_mongodb_date_from_datetime
from utils.setup_logger import main_logger


class ValueAnalysis:
    def __init__(self, column_name: str, values: Series, expected_type: str, accepted_values: list):
        self.column_name = column_name
        self.values = values
        self.expected_type = expected_type
        # we ask for sets to have good performances, especially because we need to check whether values are
        # accepted based on a set of accepted values.
        # such an operation is in O(n) for a list and O(1) or a set
        self.unique_values = set(values.unique())
        self.accepted_values = set(accepted_values)
        self.columns_to_check = {}  # columns for which the value analysis has revealed problems

        # cumulative variables to count what happens in the value analysis
        self.nb_unrecognized_data_types = 0
        self.nb_wrongly_typed_values_in_column = 0
        self.nb_empty_values = 0
        self.ratio_empty_values = 0.0
        self.ratio_values_matching_accepted = 0.0
        self.ratio_non_empty_values_matching_accepted = 0.0

    def run_analysis(self):
        self.compare_values_with_expected_type()  # this will call compare values for categorical values

    def compare_values_with_expected_type(self):
        if is_not_empty(variable=self.expected_type):
            if is_equal_insensitive(value=self.expected_type, compared="category"):
                # for categorical values, we have a dedicated method to check whether the values match the expected ones
                self.compare_values_with_accepted_values()
            else:
                type_is_int = is_equal_insensitive(value=self.expected_type, compared="int")
                type_is_float = is_equal_insensitive(value=self.expected_type, compared="float")
                type_is_datetime = is_equal_insensitive(value=self.expected_type, compared="datetime64") or is_equal_insensitive(value=self.expected_type, compared="datetime")
                type_is_str = is_equal_insensitive(value=self.expected_type, compared="str")
                wrong_type = False  # when a mistyped value is encountered this values false
                if not type_is_int and not type_is_float and not type_is_datetime and not type_is_str:
                    main_logger.error("Unrecognized type variable '%s'.", self.expected_type)
                    self.nb_unrecognized_data_types += 1
                else:
                    # for primitive types, we check that all values can be converted to the expected type (or are NaN)
                    if type_is_int:
                        for value in self.unique_values:
                            # check that all values can be cast to int or are NaN
                            value_is_not_nan = is_not_nan(value=value)
                            int_value = get_int_from_str(str_value=value)
                            if value_is_not_nan and int_value is None:
                                main_logger.debug("Could not convert %s to int value", value)
                                wrong_type = True
                    elif type_is_float:
                        for value in self.unique_values:
                            # check that all values can be cast to float or are NaN
                            value_is_not_nan = is_not_nan(value=value)
                            float_value = get_float_from_str(str_value=value)
                            if value_is_not_nan and float_value is None:
                                main_logger.debug("Could not convert %s to float value", value)
                                wrong_type = True
                    elif type_is_datetime:
                        for value in self.unique_values:
                            # check that all values can be cast to float or are NaN
                            value_is_not_nan = is_not_nan(value=value)
                            datetime_value = get_mongodb_date_from_datetime(current_datetime=value)
                            if value_is_not_nan and datetime_value is None:
                                main_logger.debug("Could not convert %s to datetime value", value)
                                wrong_type = True
                    elif type_is_str:
                        for value in self.unique_values:
                            # check that all values can be cast to str or are NaN
                            value_is_not_nan = is_not_nan(value=value)
                            if value_is_not_nan and not isinstance(value, str):
                                main_logger.debug("Could not convert %s to string value", value)
                                wrong_type = True

                    if wrong_type:
                        # some wrong types have been detected
                        main_logger.error("Some wrong type have been detected for %s", self.column_name)
                        self.nb_wrongly_typed_values_in_column += 1
                    else:
                        # no wrong type has been detected
                        main_logger.debug("No wrong type detected for %s", self.column_name)

    def compare_values_with_accepted_values(self):
        # we only compare the SET of values with the SET of accepted values
        # then, we compute the number of values matching an accepted values using the number of occurrences of each
        # in the LIST of values
        main_logger.debug(self.accepted_values)
        if is_not_empty(variable=self.accepted_values):
            matching_values = []
            for value in self.unique_values:
                # we only iterate on the distinct set of values, e.g., no need to compare twice 'Cesarean')
                for accepted_value in self.accepted_values:
                    if is_equal_insensitive(value=value, compared=accepted_value):
                        matching_values.append(value)
                        break  # do not continue to check other accepted values as we found one
            if len(matching_values) < len(self.values):
                total_nb_mathing_value = 0
                for matching_value in matching_values:
                    # compute the real number of values for which this value matches
                    total_nb_mathing_value += self.values.value_counts()[matching_value]
                nb_values = len(self.values)
                self.nb_empty_values = self.values.isna().sum()
                self.ratio_empty_values = self.nb_empty_values / len(self.values)
                main_logger.debug("Number of empty values: %s (%s)", self.nb_empty_values, self.ratio_empty_values)
                self.ratio_values_matching_accepted = total_nb_mathing_value/nb_values
                main_logger.debug("Ratio of values matching an accepted value: %s/%s=%s", total_nb_mathing_value, nb_values, self.ratio_values_matching_accepted)
                self.ratio_non_empty_values_matching_accepted = total_nb_mathing_value/(nb_values - self.nb_empty_values)
                main_logger.debug("Ratio of non-empty values matching an accepted value: %s/(%s-%s)=%s", total_nb_mathing_value, nb_values, self.nb_empty_values, self.ratio_non_empty_values_matching_accepted)
        else:
            main_logger.debug("No categorical values are expected...")

    def write_results_in_file(self):
        pass

    def to_json(self):
        return {
            "nb_unrecognized_data_types": str(self.nb_unrecognized_data_types),
            "nb_wrongly_typed_values_in_column": str(self.nb_wrongly_typed_values_in_column),
            "nb_empty_values": str(self.nb_empty_values),
            "ratio_empty_values": str(self.ratio_empty_values),
            "ratio_values_matching_accepted": str(self.ratio_values_matching_accepted),
            "ratio_non_empty_values_matching_accepted": str(self.ratio_non_empty_values_matching_accepted)
        }

    def __repr__(self):
        return json.dumps(self.to_json())
