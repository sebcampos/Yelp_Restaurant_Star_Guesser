from . import utils
import atexit

# start connection
utils.build_connection()

# display connection status and databases
print(f"Connection Successful {utils.get_connection()}")
print("Databases:")
for database in utils.get_connection().list_database_names():
    print(database)

atexit.register(utils.tear_down)
