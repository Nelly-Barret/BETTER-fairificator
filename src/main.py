import logging
import os.path
import pathlib
import sys
import argparse
import shutil
import traceback

from database.Database import Database

sys.path.append('.')  # add the current project to the python path to be runnable in cmd-line

from src.config.BetterConfig import BetterConfig
from src.etl.ETL import ETL
from src.utils.HospitalNames import HospitalNames
from src.utils.constants import DEFAULT_DB_NAME
from src.utils.setup_logger import log


if __name__ == '__main__':
    # create a config file using properties.ini
    config = BetterConfig()

    # the code is supposed to be run like this:
    # python3 main.py <hospital_name> <path/to/data.csv> <drop_db>
    parser = argparse.ArgumentParser()
    parser.add_argument("--hospital_name", help="Set the hospital name among " + str([hn.value for hn in HospitalNames]), choices={hn.value for hn in HospitalNames})
    parser.add_argument("--metadata_filepath", help="Set the absolute path to the metadata file.")
    parser.add_argument("--data_filepath", help="Set the absolute path to the data file.")
    parser.add_argument("--database_name", help="Set the database name.")
    parser.add_argument("--drop", help="Whether to drop the database.", choices={"True", "False"})
    parser.add_argument("--connection", help="The connection string to the mongodb server.")

    args = parser.parse_args()
    if args.hospital_name is not None:
        config.set_hospital_name(args.hospital_name)
    if args.connection is not None:
        config.set_db_connection(args.connection)
    if args.database_name is not None and args.database_name != "":
        config.set_db_name(args.database_name)
    else:
        log.info("There was no database name provided. Using the default one: %s", DEFAULT_DB_NAME)
        config.set_db_name(DEFAULT_DB_NAME)
    if args.drop is not None:
        config.set_db_drop(args.drop)

    # create a new folder within the tmp dir to store the current execution tmp files and config
    # this folder is named after the DB name (instead of a timestamp, which will create one folder at each run)
    working_folder = os.path.join(config.get_working_dir(), config.get_db_name())
    config.set_working_dir_current(working_folder)
    if os.path.exists(working_folder):
        shutil.rmtree(working_folder)  # empty the current working directory if it exists
    os.makedirs(working_folder)  # create the working folder (labelled with the DB name)

    # get metadata and data filepaths
    if args.metadata_filepath is None:
        log.error("No metadata file path has been provided. Please provide one.")
        exit()
    elif not os.path.isfile(args.metadata_filepath):
        log.error("The specified metadata file does not seem to exist. Please check the path.")
        exit()
    else:
        metadata_filename = "metadata-" + args.hospital_name + ".csv"
        metadata_filepath = os.path.join(config.get_working_dir_current(), metadata_filename)
        shutil.copyfile(args.metadata_filepath, metadata_filepath)
        config.set_metadata_filepath(metadata_filepath)

    if args.data_filepath is None:
        log.error("No data file path has been provided. Please provide one.")
        exit()
    else:
        # if there is a single file, this will put that file in a list
        # otherwise, when the user provides several files, it will split them in the array
        log.debug(args.data_filepath)
        split_files = args.data_filepath.split(",")
        log.debug(split_files)
        for current_file in split_files:
            if not os.path.isfile(current_file):
                log.error("The specified data file '%s' does not seem to exist. Please check the path.", current_file)
                exit()
        # we do not copy the data in our working dir because it is too large to be copied
        config.set_data_filepaths(args.data_filepath)  # file 1,file 2, ...,file N
        log.debug(config.get_data_filepaths())

    # write more information about the current run in the config
    config.add_python_version()
    config.add_pymongo_version()
    config.add_execution_date()
    config.add_platform()
    config.add_platform_version()
    config.add_user()

    # save the config file in the current working directory
    config.write_to_file()

    # print the main parameters of the current run
    log.info("Selected hospital name: %s", config.get_hospital_name())
    log.info("The database name is %s", config.get_db_name())
    log.info("The connection string is: %s", config.get_db_connection())
    log.info("The database will be dropped: %s", config.get_db_drop())
    log.info("The metadata file is located at: %s", config.get_metadata_filepath())
    log.info("The data file is located at: %s", config.get_data_filepaths())
    log.debug(config.to_json())

    database = Database(config=config)

    error_occurred = False
    all_datapaths = config.get_data_filepaths()
    for one_file in all_datapaths:
        log.debug(one_file)
        # set the current path in the config because the ETL only knows files declared in the config
        # we need to add twice .. because the data files are never copied to the working dir (but remain in their place)
        config.set_current_filepath(os.path.join(config.get_working_dir_current(), "..", "..", one_file))
        log.info("--- Starting to ingest file '%s'", config.get_data_filepaths())
        try:
            etl = ETL(config=config, database=database)
            etl.run()
        except Exception as error:
            traceback.print_exc()  # print the stack trace
            log.error("An error occurred during the ETL. Please check the complete log. ")
            error_occurred = True

    if not error_occurred:
        log.info("All given files have been processed without error. Goodbye!")
    else:
        log.error("The script stopped at some point due to errors. Please check the complete log.")

    # everything has been written in the log file,
    # so we move it (the file with the latest timestamp) to its respective database folder in working-dir
    # first: close handlers that were writing the log files
    log.handlers.clear()
    # now we can move the latest log file to its destination
    latest_log_filename = max([f for f in pathlib.Path('.').glob('*.log')], key=os.path.getctime)
    shutil.move(latest_log_filename, os.path.join(config.get_working_dir_current(), latest_log_filename))
