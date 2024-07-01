import logging
import os
import pathlib
import shutil
from time import strftime

# Create two loggers
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

# first logger
main_logger = logging.getLogger("main_logger")
main_logger.setLevel(logging.DEBUG)
main_handler_1 = logging.FileHandler('log-{}.log'.format(strftime('%Y-%m-%d:%H:%M:%S')))
main_handler_1.setFormatter(formatter)
main_logger.addHandler(main_handler_1)
main_handler_2 = logging.StreamHandler()
main_handler_2.setFormatter(formatter)
main_logger.addHandler(main_handler_2)

# second logger
# test_logger = logging.getLogger("test_logger")
# test_logger.setLevel(logging.DEBUG)
# test_handler = logging.StreamHandler()
# test_handler.setFormatter(formatter)
# test_logger.addHandler(test_handler)


# logging.basicConfig(
#     level=logging.DEBUG,
#     format='%(asctime)s, %(levelname)-5s [%(module)s:%(funcName)s:%(lineno)d] %(message)s',
#     datefmt='%Y-%m-%d:%H:%M:%S',
#     handlers=[
#         logging.FileHandler('log-{}.log'.format(strftime('%Y-%m-%d:%H:%M:%S'))),
#         logging.StreamHandler()
#     ]
# )
# log = logging.getLogger()


# do not allow PyMongo to print everything,
# only important messages (warning, error and fatal) wil be shown
logging.getLogger("pymongo").setLevel(logging.WARNING)
logging.getLogger("matplotlib").setLevel(logging.WARNING)
logging.getLogger("PIL").setLevel(logging.WARNING)


def finish_with_logging(my_logger, current_working_dir: str):
    my_logger.handlers.clear()
    # now we can move the latest log file to its destination
    latest_log_filename = max([f for f in pathlib.Path('.').glob('*.log')], key=os.path.getctime)
    shutil.move(latest_log_filename, os.path.join(current_working_dir, latest_log_filename))
