from . import utils
import atexit

utils.create_connections()

atexit.register(utils.tear_down)