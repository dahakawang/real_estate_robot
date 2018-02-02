import pymongo

class Mongo:
    @staticmethod
    def get_client(cfg):
        return pymongo.MongoClient(cfg["mongo"]["url"])
