from pymongo import MongoClient

def delete_many_not_in_batch(db, dorm_names):
    for dorm_name in dorm_names:
        db.dormitory.delete_one({'dorm_name': dorm_name})

def delete_many_in_batch(db,  dorm_names):
    db.dormitory.delete_many({'dorm_name': {'$in': dorm_names}})

def delete_one(db, dorm_name):
    db.dormitory.delete_one({'dorm_name': dorm_name})


if __name__ == "__main__":
    client = MongoClient("mongodb://admin:password@localhost:27017/")
    db = client["dormitory_management_system"]

    dorm_names = [f"Dormitory {i}" for i in range(1, 10)]
    delete_many_not_in_batch(db, dorm_names)

    dorm_names = [f"Dormitory {i}" for i in range(10, 20)]
    delete_many_in_batch(db,dorm_names)

    dorm_name = "Dormitory 3979"
    delete_one(db,dorm_name)