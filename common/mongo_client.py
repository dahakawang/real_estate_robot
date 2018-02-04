import pymongo

class Mongo:
    @staticmethod
    def setup_client(cfg):
        client = pymongo.MongoClient(cfg["mongo"]["url"])
        cfg["mongo"] = client
