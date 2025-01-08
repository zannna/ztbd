from pymongo import MongoClient

class DeleteMongo:
    @staticmethod
    def delete_many_not_in_batch(db, dormitories):
        for dormitory in dormitories:
            db.dormitory.delete_one({'dorm_name': dormitory})

    @staticmethod
    def delete_many_in_batch(db, dormitories):
        db.dormitory.delete_many({'dorm_name': {'$in': dormitories}})

    @staticmethod
    def delete_one(db, dormitory):
        db.dormitory.delete_one({'dorm_name': dormitory})


if __name__ == "__main__":
    client = MongoClient("mongodb://admin:password@localhost:27017/")
    database = client["dormitory_management_system"]

    dorm_names = [f"Dormitory {i}" for i in range(1, 10)]
    DeleteMongo.delete_many_not_in_batch(database, dorm_names)

    dorm_names = [f"Dormitory {i}" for i in range(10, 20)]
    DeleteMongo.delete_many_in_batch(database, dorm_names)

    dorm_name = "Dormitory 3979"
    DeleteMongo.delete_one(database, dorm_name)