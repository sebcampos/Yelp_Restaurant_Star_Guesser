import ftplib
from . import variables
from . import constants
import pandas as pd
import sqlite3


def tear_down():
    # close connections
    variables.ftp_client.close()
    variables.sql_conn.close()


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


def read_completed():
    # collect list of completed
    return pd.read_sql("select completed_dirs from temp", con=variables.sql_conn).completed_dirs.tolist()


def add_to_final(filename, path):
    df = pd.DataFrame(
        {
            "filename": [filename],
            "path": [path]
        })
    df.to_sql("result", con=variables.sql_conn, if_exists="append")
    print(f"{filename} added with path: {path}")


def iterate():

    for i in variables.ftp_client.nlst():
        if is_dir(i) and f"{variables.ftp_client.pwd()}/{i}" not in read_completed():
            variables.ftp_client.cwd(i)
            iterate()

    base_dir = variables.ftp_client.pwd()

    if len(variables.ftp_client.nlst()) == 0:
        add_to_final("empty", base_dir)
    else:
        for filename in variables.ftp_client.nlst():
            add_to_final(filename, base_dir+"/"+filename)

    add_to_completed(base_dir)
    variables.ftp_client.cwd("..")
    if variables.ftp_client.pwd() == base_dir:
        return 0
    iterate()


