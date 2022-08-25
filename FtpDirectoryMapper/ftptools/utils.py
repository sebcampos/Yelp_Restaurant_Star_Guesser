import ftplib
import logging
from . import variables
from . import constants
import pandas as pd
import sqlite3


def init_logger():
    log_format = f"[%(asctime)s][%(name)s][%(levelname)s\t] - %(message)s"

    logging.basicConfig(
        format=log_format,
    )

    logger = logging.getLogger("FTPLog")
    logger.setLevel("INFO")
    return logger


def tear_down():
    # close connections
    variables.ftp_client.close()
    variables.sql_conn.close()
    variables.logger.info("Connections closed successfully")


def create_connections():
    # set up ftp connection
    variables.ftp_client = ftplib.FTP(
        constants.SITE_ADDRESS,
        constants.FTP_USER,
        constants.FTP_PASS
    )
    variables.ftp_client.connect(constants.SITE_ADDRESS, constants.FTP_PORT)
    variables.ftp_client.login(constants.FTP_USER, constants.FTP_PASS)

    # set up sqlite3 connection
    variables.sql_conn = sqlite3.connect(constants.DB_NAME)
    pd.DataFrame({"completed_dirs": []}).to_sql("temp", con=variables.sql_conn, if_exists="replace")

    variables.logger = init_logger()


def is_dir(file):
    try:
        variables.ftp_client.cwd(file)
        variables.ftp_client.cwd("..")
        return True
    except Exception as e:
        return False


def add_to_completed(completed):
    # add directory to completed
    pd.DataFrame({"completed_dirs": [completed]}) \
        .to_sql("temp", con=variables.sql_conn, if_exists="append")
    variables.logger.info(f"completed iteration of {completed}")


def read_completed():
    # collect list of completed
    return pd.read_sql("select completed_dirs from temp", con=variables.sql_conn).completed_dirs.tolist()


def add_to_final(filename, path):
    if filename == "empty":
        variables.logger.warn(f"{path} and {filename} is empty")
    df = pd.DataFrame(
        {
            "filename": [filename],
            "path": [path]
        })
    df.to_sql("result", con=variables.sql_conn, if_exists="append")
    variables.logger.info(f"{filename} added with path: {path}")


def move_to_next_directory():
    for i in variables.ftp_client.nlst():
        if is_dir(i) and f"{variables.ftp_client.pwd()}/{i}" not in read_completed():
            variables.ftp_client.cwd(i)
            return True
    return False


def add_files_in_current_directory(current_dir):
    for filename in variables.ftp_client.nlst():
        if not is_dir(filename):
            add_to_final(filename, current_dir + "/" + filename)


def iterate(starting_directory=False):
    if starting_directory:
        variables.ftp_client.cwd(starting_directory)
    else:
        starting_directory = variables.ftp_client.pwd()

    starting_directory_size = len(variables.ftp_client.nlst())
    starting_directory_completed = 0
    while starting_directory_size != starting_directory_completed:
        if move_to_next_directory():
            continue

        current_dir = variables.ftp_client.pwd()
        if len(variables.ftp_client.nlst()) == 0:
            add_to_final("empty", current_dir)
        else:
            add_files_in_current_directory(current_dir)

        add_to_completed(current_dir)
        completed_directory = current_dir
        variables.ftp_client.cwd("..")
        if completed_directory == starting_directory:
            starting_directory_completed += 1
            variables.logger.info(f"completed {starting_directory_completed} of {starting_directory_size} in main "
                                  f"directory") 

    return 0
