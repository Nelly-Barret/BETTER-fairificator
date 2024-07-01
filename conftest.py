import os.path

from utils.setup_logger import main_logger, finish_with_logging


def pytest_sessionfinish(session, exitstatus):
    """
    Called after whole test run finished, right before
    returning the exit status to the system.
    """
    main_logger.info("after all tests")  # Your code goes here
    finish_with_logging(my_logger=main_logger, current_working_dir=os.path.join(BetterConfig., "better_default"))
