import json


def yelp_json_load(filename: str) -> list:
    """
    This method reads and parses json files provided by
    yelp. each entry is a json data point seperated by the
    "\n". This method splits each entry by the new line and
    utilizes the json loads to load the string json value
    as a python dictionary. each entry is loaded as a python dict
    then added to a python list.
    :param filename: json file to be loaded from the yelp dataset
    :return: a list of python dictionaries representing the file
    """
    with open(filename, "r") as f:
        return [json.loads(line) for line in f.readlines()]


def write_json(filename: str, json_data: list or dict) -> None:
    """
    This method takes in a python object, converts it to json
    and writes a new json file with said data
    :param filename: name of the new json file
    :param json_data: python object to be converted
    :return: void
    """
    with open(filename, "w") as f:
        json.dump(json_data, f, indent=6)
