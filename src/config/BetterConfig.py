import configparser
import getpass
import os
import platform
import sys
from datetime import datetime

import pymongo

from src.utils.constants import DEFAULT_CONFIG_FILE
from src.utils.setup_logger import log
from src.utils.utils import is_not_empty


class BetterConfig:
    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read(DEFAULT_CONFIG_FILE)

    # below, define methods for each parameter in the config
    # keep it up-to-date wrt the config file
    def set_working_dir(self, working_dir):
        if is_not_empty(working_dir):
            self.set_files_section()
            self.config.set("FILES", "working_dir", working_dir)
        else:
            log.error("The working dir cannot be set in the config because it is None or empty.")

    def set_working_dir_current(self, working_dir_current):
        if is_not_empty(working_dir_current):
            self.set_files_section()
            self.config.set("FILES", "working_dir_current", working_dir_current)
        else:
            log.error("The current working dir cannot be set in the config because it is None or empty.")

    def set_metadata_filepath(self, metadata_filepath):
        if is_not_empty(metadata_filepath):
            self.set_files_section()
            self.config.set("FILES", "metadata_filepath", metadata_filepath)
        else:
            log.error("The metadata filepath cannot be set in the config because it is None or empty.")

    def set_data_filepath(self, data_filepath):
        if is_not_empty(data_filepath):
            self.set_files_section()
            self.config.set("FILES", "data_filepath", data_filepath)
        else:
            log.error("The data filepath cannot be set in the config because it is None or empty.")

    def set_db_connection(self, db_connection):
        if is_not_empty(db_connection):
            self.set_database_section()
            self.config.set("DATABASE", "connection", db_connection)
        else:
            log.error("The db connection string cannot be set in the config because it is None or empty.")

    def set_db_name(self, db_name):
        if is_not_empty(db_name):
            self.set_database_section()
            self.config.set("DATABASE", "name", db_name)
        else:
            log.error("The db name cannot be set in the config because it is None or empty.")

    def set_db_drop(self, drop):
        if is_not_empty(drop):
            self.set_database_section()
            self.config.set("DATABASE", "drop", drop)
        else:
            log.error("The drop db cannot be set in the config because it is None or empty.")

    def set_hospital_name(self, hospital_name):
        if is_not_empty(hospital_name):
            self.set_hospital_section()
            self.config.set("HOSPITAL", "name", hospital_name)
        else:
            log.error("The hospital cannot be set in the config because it is None or empty.")

    def add_python_version(self):
        python_version = str(sys.version)
        if is_not_empty(python_version):
            self.set_system_section()
            self.config.set("SYSTEM", "python_version", python_version)
        else:
            log.error("The Python version cannot be set in the config because it is None or empty.")

    def add_pymongo_version(self):
        pymongo_version = pymongo.version
        if is_not_empty(pymongo_version):
            self.set_system_section()
            self.config.set("SYSTEM", "pymongo_version", pymongo_version)
        else:
            log.error("The Pymongo version cannot be set in the config because it is None or empty.")

    def add_mongodb_version(self, client):
        mongodb_version = client.server_info()["version"]
        if is_not_empty(mongodb_version):
            self.set_system_section()
            self.config.set("SYSTEM", "mongodb_version", mongodb_version)
        else:
            log.error("The Pymongo version cannot be set in the config because it is None or empty.")

    def add_execution_date(self):
        execution_date = str(datetime.now())
        if is_not_empty(execution_date):
            self.set_system_section()
            self.config.set("SYSTEM", "execution_date", execution_date)
        else:
            log.error("The execution date cannot be set in the config because it is None or empty.")

    def add_platform(self):
        platform_value = platform.platform()
        if is_not_empty(platform_value):
            self.set_system_section()
            self.config.set("SYSTEM", "platform", platform_value)
        else:
            log.error("The platform cannot be set in the config because it is None or empty.")

    def add_platform_version(self):
        platform_version = platform.version()
        if is_not_empty(platform_version):
            self.set_system_section()
            self.config.set("SYSTEM", "platform_version", platform_version)
        else:
            log.error("The plateform version cannot be set in the config because it is None or empty.")

    def add_user(self):
        user = getpass.getuser()
        if is_not_empty(user):
            self.set_system_section()
            self.config.set("SYSTEM", "user", user)
        else:
            log.error("The user cannot be set in the config because it is None or empty.")

    # set sections
    def set_files_section(self):
        if not self.config.has_section("FILES"):
            self.config.add_section("FILES")

    def set_database_section(self):
        if not self.config.has_section("DATABASE"):
            self.config.add_section("DATABASE")

    def set_hospital_section(self):
        if not self.config.has_section("HOSPITAL"):
            self.config.add_section("HOSPITAL")

    def set_system_section(self):
        if not self.config.has_section("SYSTEM"):
            self.config.add_section("SYSTEM")

    # get config variables
    def get_working_dir(self):
        try:
            return self.config.get("FILES", "working_dir")
        except:
            # if the section or the key is not found, return empty string
            return ""

    def get_working_dir_current(self):
        try:
            return self.config.get("FILES", "working_dir_current")
        except:
            return ""

    def get_metadata_filepath(self):
        try:
            return self.config.get("FILES", "metadata_filepath")
        except:
            return ""

    def get_data_filepath(self):
        try:
            return self.config.get("FILES", "data_filepath")
        except:
            return ""

    def get_db_connection(self):
        try:
            return self.config.get("DATABASE", "connection")
        except:
            return ""

    def get_db_name(self):
        try:
            return self.config.get("DATABASE", "name")
        except:
            return ""

    def get_db_drop(self) -> bool:
        try:
            return True if self.config.get("DATABASE", "drop") == "True" else False
        except:
            return False

    def get_hospital_name(self):
        try:
            return self.config.get("HOSPITAL", "name")
        except:
            return ""

    def get_python_version(self):
        try:
            return self.config.get("SYSTEM", "python_version")
        except:
            return ""

    def get_pymongo_version(self):
        try:
            return self.config.get("SYSTEM", "pymongo_version")
        except:
            return ""

    def get_mongodb_version(self):
        try:
            return self.config.get("SYSTEM", "mongodb_version")
        except:
            return ""

    def get_execution_date(self):
        try:
            return self.config.get("SYSTEM", "execution_date")
        except:
            return ""

    def get_platform(self):
        try:
            return self.config.get("SYSTEM", "platform")
        except:
            return ""

    def get_platform_version(self):
        try:
            return self.config.get("SYSTEM", "platform_version")
        except:
            return ""

    def get_user(self):
        try:
            return self.config.get("SYSTEM", "user")
        except:
            return ""

    # write config to file
    def write_to_file(self):
        log.debug(os.path.join(self.get_working_dir_current(), DEFAULT_CONFIG_FILE))
        with open(os.path.join(self.get_working_dir_current(), DEFAULT_CONFIG_FILE), 'w') as f:
            self.config.write(f)

    def to_json(self):
        return {
            "FILES": [
                { "working_dir": self.get_working_dir() },
                { "working_dir_current": self.get_working_dir_current() },
                { "metadata_filepath": self.get_metadata_filepath() },
                { "data_filepath": self.get_data_filepath() }
            ],
            "DATABASE": [
                { "connection": self.get_db_connection() },
                { "name": self.get_db_name() },
                { "drop": self.get_db_drop() }
            ],
            "HOSPITAL": [
                { "name": self.get_hospital_name() }
            ],
            "SYSTEM": [
                { "python_version": self.get_python_version() },
                { "pymongo_version": self.get_pymongo_version() },
                { "mongodb_version": self.get_mongodb_version() },
                { "execution_date": self.get_execution_date() },
                { "platform": self.get_platform() },
                { "platform_version": self.get_platform_version() },
                { "user": self.get_user()}
            ]
        }
