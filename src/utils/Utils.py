import math
from typing import Any

from dateutil.parser import parse
from pandas import DataFrame

from src.fhirDatatypes.CodeableConcept import CodeableConcept
from src.fhirDatatypes.Identifier import Identifier
from src.fhirDatatypes.Reference import Reference
from src.utils.Ontologies import Ontologies


# ASSERTIONS

def is_float(value) -> bool:
    if isinstance(value, str) or isinstance(value, int) or isinstance(value, float):
        try:
            float(value)
            return True
        except ValueError:
            return False
    else:
        # for dict, lists, etc
        return False


def is_not_nan(value) -> bool:
    return not is_float(value) or (is_float(value) and not math.isnan(float(value)))


def assert_not_empty(variable, not_empty=True) -> None:
    if is_not_empty(variable=variable, not_empty=not_empty):
        pass
    else:
        message = "A variable " + variable + " is not supposed to be empty"
        assert False, message


def is_not_empty(variable, not_empty=True) -> bool:
    if isinstance(variable, int) or isinstance(variable, float):
        return True
    elif isinstance(variable, str):
        return variable != ""
    elif isinstance(variable, list):
        if not_empty:
            return variable is not None and variable != []
        else:
            return variable is not None
    elif isinstance(variable, dict):
        if not_empty:
            return variable is not None and variable != {}
        else:
            return variable is not None
    elif isinstance(variable, tuple):
        if not_empty:
            return variable is not None and variable != ()
        else:
            return variable is not None
    elif isinstance(variable, DataFrame):
        return not variable.empty
    elif isinstance(variable, set):
        return variable != set()
    else:
        # no clue about the variable typ
        # thus, we only check whether it is None
        return variable is not None


def is_equal_insensitive(value, compared):
    if not isinstance(value, str):
        return value == compared
    else:
        return value.casefold() == compared.casefold()


# BUILDING URLs


# def build_url(base: str, element_id: str) -> str:
#     assert_type(base, str)
#     assert_type(element_id, int)
#
#     return base + "/" + str(element_id)


# GET PREDEFINED VALUES


def get_ontology_system(ontology: str) -> str:
    ontology = normalize_value(ontology)

    for existing_ontology in Ontologies:
        if existing_ontology.value["name"] == ontology:
            return existing_ontology.value["url"]  # return the ontology URI associated to that ontology
    # at the end of the loop, no enum value could match the given ontology
    # thus we need to raise an error
    raise ValueError("The given ontology system '%s' is not known.", ontology)


def get_ontology_resource_uri(ontology_system: str, resource_code: str) -> str:
    return ontology_system + "/" + resource_code


def get_identifier_from_json(identifier_as_json: dict, resource_type: str):
    return Identifier(id_value=identifier_as_json["value"], resource_type=resource_type, use=identifier_as_json["use"])


def get_codeable_concept_from_json(codeable_concept_as_json: dict):
    cc = CodeableConcept()
    cc.text = codeable_concept_as_json["text"]
    for coding_as_json in codeable_concept_as_json["coding"]:
        cc.add_coding((coding_as_json["system"], coding_as_json["code"], codeable_concept_as_json["display"]))
    return cc


def get_category_from_json(category_as_json: dict):
    # the category is either PHENOTYPIC or CLINICAL, thus no loop for the codings is necessary
    return CodeableConcept((category_as_json["system"], category_as_json["code"], category_as_json["display"]))


def get_reference_from_json(reference_as_json: dict):
    return Reference(get_identifier_from_json(identifier_as_json=reference_as_json["reference"], resource_type=reference_as_json["type"]))


# NORMALIZE DATA

def get_int_from_str(str_value) -> int:
    try:
        return int(str_value)
    except ValueError:
        pass  # this was not an int value


def get_float_from_str(str_value) -> float:
    try:
        return float(str_value)
    except ValueError:
        pass  # this was not a float value


def get_datetime_from_str(str_value) -> str:
    try:
        datetime_value = parse(str_value)
        # %Y-%m-%d %H:%M:%S is the format used by default by parse (the output is always of this form)
        if ":" in str_value:
            # there was a time in the value, let's return a datetime value
            return str(datetime_value)
        else:
            # the value was only a date, so we return only a date too
            return str(datetime_value.date())
    except ValueError:
        pass  # this was not a datetime value


def cast_value(value):
    if isinstance(value, str):
        # trying to cast to something if possible
        # first, try to cast as int
        int_value = get_int_from_str(value)
        if int_value is not None:
            return int_value

        # try to cast as float
        float_value = get_float_from_str(value)
        if float_value is not None:
            return float_value

        # try to cast as datetime (first, because it is more restrictive than simple date)
        datetime_value = get_datetime_from_str(value)
        if datetime_value is not None:
            return datetime_value
    else:
        # log.info("%s is not a string, so no further cast is possible", value)
        return value


def normalize_value(input_string: str) -> str:
    return input_string.upper().strip().replace(" ", "").replace("_", "")


# MONGODB UTILS

def mongodb_match(field: str, value: Any) -> dict:
    return {
        "$match": {
            field: value
        }
    }


def mongodb_project_one(field: str) -> dict:
    return {
        "$project": {
            field: 1
        }
    }


def mongodb_sort(field: str, sort_order: int) -> dict:
    return {
        "$sort": {
            field: sort_order
        }
    }


def mongodb_limit(nb: int) -> dict:
    return {
        "$limit": nb
    }


def mongodb_group_by(group_key: Any, group_by_name: str, operator: str, field) -> dict:
    return {
        "$group": {
            "_id": group_key,
            group_by_name: {
                operator: "$" + field  # $avg: $<the field on which the avg is computed>
            }
        }
    }


# LIST AND DICT CONVERSIONS


def get_values_from_json_values(json_values):
    values = []
    for current_dict in json_values:
        if is_not_nan(current_dict) and is_not_empty(current_dict):
            values.append(current_dict["value"])
    return values


def convert_value(value):
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value
