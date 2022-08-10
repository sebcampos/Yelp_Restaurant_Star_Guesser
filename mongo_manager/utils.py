from . import constants
from . import variables
from pymongo import MongoClient
from pandas import DataFrame


def build_connection(local: bool = True) -> None:
    """
    This method sets the mongo db connection var
    :param local: boolean determines connection url
    :return: void
    """
    if local:
        connection_string = constants.LOCAL_MONGO_DB
    else:
        connection_string = constants.MONGO_CLUSTER_URL
    variables.mongo_client_connection = MongoClient(connection_string)


def get_connection() -> MongoClient:
    return variables.mongo_client_connection


def tear_down() -> None:
    """
    This method closes the connection
    to the mongodb
    :return: void
    """
    variables.mongo_client_connection.close()
    print("Connection to mongodb closed")


def read_collection(database_name: str, collection_name: str) -> DataFrame:
    """
    This method finds a mongo collection by database name and collection name.
    then it converts the collection into a pandas DataFrame before returning it
    :param database_name: name of the database
    :param collection_name: name of the collection
    :return: Pandas DataFrame
    """
    cursor = variables.mongo_client_connection[database_name][collection_name].find()
    df = DataFrame(list(cursor))
    cursor.close()
    return df


def list_collections(database_name: str, void: bool = True) -> None or list:
    """
    This method lists all collections in the provided database.
    if void is false it will return the list object
    :param database_name: name of database
    :param void: boolean determining return value
    :return:
    """
    if void:
        for collection in variables.mongo_client_connection[database_name].list_collection_names():
            print(collection)
        return
    return variables.mongo_client_connection[database_name].list_collection_names()


def upload_dataframes(database_name: str, dataframes: dict) -> None:
    """
    This method takes a dictionary of dataframes where the keys are the names to be and
    the values are the dataframes. Then it will populate the specified database with
    collections where the name of the collection maps to that dataframe
    :param database_name: database name
    :param dataframes: dictionary of dataframes
    :return: void
    """
    for name, df in dataframes.items():
        upload_dataframe(database_name, name, df)


def upload_dataframe(database_name: str, collection_name: str, dataframe: DataFrame) -> None:
    """
    This method uploads one dataframe to the specified database with the name defined by collection_name
    :param database_name: name of the database
    :param collection_name: name of the new collection
    :param dataframe: dataframe to be added
    :return: void
    """
    variables.mongo_client_connection[database_name][collection_name].insert_many(dataframe.to_dict("records"))
