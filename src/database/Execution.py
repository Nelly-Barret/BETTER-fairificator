import getpass
import os.path
import platform
import shutil
from datetime import datetime

import pymongo
from argparse import Namespace

from utils.constants import WORKING_DIR, DEFAULT_DB_NAME, DEFAULT_DB_CONNECTION
from utils.setup_logger import log
from utils.utils import is_not_empty


class Execution:
    # parameters related to the project structure and the input/output files
    # parameters related to the database
    # parameters related to the UC hospital
    HOSPITAL_NAME_KEY = "hospital_name"
    # parameters related to the execution context (python, pymongo, etc)
    PYTHON_VERSION_KEY = "python_version"
    PYMONGO_VERSION_KEY = "pymongo_version"
    EXECUTION_KEY = "execution_date"
    PLATFORM_KEY = "platform"
    PLATFORM_VERSION_KEY = "platform_version"
    USER_KEY = "user"
    USE_EN_LOCALE_KEY = "locale"
    # parameters related to the ETL pipeline
    EXTRACT_KEY = "extract"
    TRANSFORM_KEY = "transform"
    LOAD_KEY = "load"
    ANALYSIS_KEY = "analysis"

    def __init__(self):
        self.created_at = datetime.now().isoformat()
        # self.execution_analysis = ExecutionAnalysis(database)
        # self.execution_analysis.run()
        # self.database = database
        # self.counter = counter

        # parameters related to the project structure and the input/output files
        self.working_dir = os.path.join(os.getcwd(), WORKING_DIR)  # default in the code
        self.working_dir_current = None  # computed in setup
        self.metadata_filepath = None  # user input
        self.data_filepaths = None  # user input
        self.current_filepath = None  # computed in setup
        self.use_en_locale = True  # user input
        # parameters related to the database
        self.db_name = DEFAULT_DB_NAME  # user input
        self.db_connection = DEFAULT_DB_CONNECTION  # user input
        self.db_drop = True  # user input
        self.db_no_index = False  # user input
        # parameters related to the UC hospital
        self.hospital_name = None  # this will be given as input by users
        # parameters related to the execution context (python, pymongo, etc)
        self.python_version = platform.python_version()
        self.pymongo_version = pymongo.version
        self.execution_date = datetime.now()
        self.platform = platform.platform()
        self.platform_version = platform.version()
        self.user = getpass.getuser()
        # parameters related to the ETL pipeline
        self.extract = True
        self.transform = True
        self.load = True
        self.analyze = False

        # if self.counter is None: # TODO Nelly: uncomment this -- maybe not
        #     self.counter = Counter()
        #     self.counter.set_with_database(self.database)

    # def store_in_database(self): # TODO Nelly: uncomment this
    #     self.database.insert_one_tuple(TableNames.EXECUTION.value, self.to_json())

    def set_up(self, args: Namespace) -> None:
        # A. set up the user parameters
        self.hospital_name = args.hospital_name
        self.use_en_locale = args.use_en_locale
        self.db_connection = args.connection
        self.db_name = args.database_name
        self.db_drop = args.drop == "True"
        self.db_no_index = args.no_index == "False"
        self.extract = args.extract == "True"
        self.analyze = args.analysis == "False"
        self.transform = args.transform == "True"
        self.load = args.load == "True"

        # B. set up the working-dir structure
        # 1. check whether the folder working-dir exists, if not create it
        current_path = os.getcwd()
        working_dir = os.path.join(current_path, WORKING_DIR)
        if not os.path.exists(working_dir):
            log.info("Creating the working dir at %s", working_dir)
            os.makedirs(working_dir)
        # 2. check whether the db folder exists, if not create it
        working_dir_with_db = os.path.join(working_dir, self.db_name)
        if not os.path.exists(working_dir_with_db):
            log.info("Creating a sub folder for the current db")
            os.makedirs(working_dir_with_db)
        # 3. check whether the execution folder exists, if not create it
        execution_folder = os.path.join(working_dir_with_db, self.execution_date.isoformat())
        if not os.path.exists(execution_folder):
            log.info("Creating a sub foldr for the current execution")
            os.makedirs(execution_folder)
        self.working_dir_current = execution_folder

        # C. set up the data and metadata files
        # get metadata and data filepaths
        if not os.path.isfile(args.metadata_filepath):
            log.error("The specified metadata file does not seem to exist. Please check the path.")
            exit()
        else:
            new_metadata_filename = "metadata-" + self.hospital_name + ".csv"
            new_metadata_filepath = os.path.join(self.working_dir_current, new_metadata_filename)
            shutil.copyfile(args.metadata_filepath, new_metadata_filepath)
            self.metadata_filepath = args.metadata_filepath

        # if there is a single file, this will put that file in a list
        # otherwise, when the user provides several files, it will split them in the array
        log.debug(args.data_filepaths)
        split_files = args.data_filepaths.split(",")
        log.debug(split_files)
        for current_file in split_files:
            if not os.path.isfile(current_file):
                log.error("The specified data file '%s' does not seem to exist. Please check the path.", current_file)
                exit()
        # we do not copy the data in our working dir because it is too large to be copied
        self.data_filepaths = split_files  # file 1,file 2, ...,file N
        log.debug(self.data_filepaths)

    def set_metadata_filepath(self, metadata_filepath: str) -> None:
        if is_not_empty(metadata_filepath):
            self.metadata_filepath = metadata_filepath
        else:
            log.error("The metadata filepath cannot be set in the config because it is None or empty.")

    def set_data_filepaths(self, data_filepaths: str) -> None:
        # data_filepaths is a set of data filepaths, concatenated with commas (,)
        # this is what we get from the user input parameters
        if is_not_empty(data_filepaths):
            self.data_filepaths = data_filepaths.split(",")
        else:
            log.error("The data filepaths cannot be set in the config because it is None or empty.")

    def set_db_connection(self, db_connection: str) -> None:
        if is_not_empty(db_connection):
            self.db_connection = db_connection
        else:
            log.error("The db connection string cannot be set in the config because it is None or empty.")

    def set_db_name(self, db_name: str) -> None:
        if is_not_empty(db_name):
            self.db_name = db_name
        else:
            log.error("The db name string cannot be set in the config because it is None or empty.")

    def set_db_drop(self, drop: bool) -> None:
        if is_not_empty(drop):
            self.db_drop = drop
        else:
            log.error("The drop parameter cannot be set in the config because it is None or empty.")

    def set_no_index(self, no_index: bool) -> None:
        if is_not_empty(no_index):
            self.db_no_index = no_index
        else:
            log.error("The no_index parameter cannot be set in the config because it is None or empty.")

    def set_hospital_name(self, hospital_name: str) -> None:
        if is_not_empty(hospital_name):
            self.hospital_name = hospital_name
        else:
            log.error("The hospital name parameter cannot be set in the config because it is None or empty.")

    def set_extract(self, extract: bool) -> None:
        self.extract = extract

    def set_analyze(self, analyze: bool) -> None:
        self.analyze = analyze

    def set_transform(self, transform: bool) -> None:
        self.transform = transform

    def set_load(self, load: bool) -> None:
        self.load = load

    # get execution variables
    def get_working_dir(self) -> str:
        return self.working_dir

    def get_working_dir_current(self) -> str:
        return self.working_dir_current

    def get_metadata_filepath(self) -> str:
        return self.metadata_filepath

    def get_current_filepath(self) -> str:
        return self.current_filepath

    def set_current_filepath(self, current_filepath: str) -> None:
        self.current_filepath = current_filepath

    def get_data_filepaths(self) -> list[str]:
        return self.data_filepaths

    def get_db_connection(self) -> str:
        return self.db_connection

    def get_db_name(self) -> str:
        return self.db_name

    def get_db_drop(self) -> bool:
        return self.db_drop

    def get_no_index(self) -> bool:
        return self.db_no_index

    def get_hospital_name(self) -> str:
        return self.hospital_name

    def get_pymongo_version(self) -> str:
        return self.pymongo_version

    def get_execution_date(self) -> datetime:
        return self.execution_date

    def get_platform(self) -> str:
        return self.platform

    def get_platform_version(self) -> str:
        return self.platform_version

    def get_user(self) -> str:
        return self.user

    def get_use_en_locale(self) -> bool:
        return self.use_en_locale

    def get_extract(self) -> bool:
        return self.extract

    def get_transform(self) -> bool:
        return self.transform

    def get_load(self) -> bool:
        return self.load

    def get_analyze(self) -> bool:
        return self.analyze

    def to_json(self):
        return {
            # "identifier": self.identifier.to_json(),  # TODO Nelly: check how to number Execution instances
            "created_at": self.created_at,
            "user_parameters": {
                "working_dir": self.working_dir,
                "working_dir_current": self.working_dir_current,
                "metadata_filepath": self.metadata_filepath,
                "data_filepaths": self.data_filepaths,
                "current_filepath": self.current_filepath,
                "db_connection": self.db_connection,
                "db_name": self.db_name,
                "db_drop": self.db_drop,
                "hospital_name": self.hospital_name,
            },
            "execution_context": {
                "python_version": self.python_version,
                "pymongo_version": self.pymongo_version,
                "execution_date": self.execution_date,
                "platform": self.platform,
                "platform_version": self.platform_version,
                "user": self.user
            },
            # "analysis": self.execution_analysis.to_json()  # TODO Nelly: uncomment this
        }
