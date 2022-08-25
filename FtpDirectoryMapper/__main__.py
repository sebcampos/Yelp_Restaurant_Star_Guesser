import __init__


utils.create_connections()
print("Connections made...")

atexit.register(utils.tear_down)
print("Tear down registered")

utils.iterate()