import project
from pymongo import MongoClient

project_client = MongoClient(project.MONGOSERVER + ":" + project.MONGOPORT)
project_server = project.MONGOSERVER
project_port = project.MONGOPORT
project_db = project_client.udacity
project_coll = project_db.osmnodes
