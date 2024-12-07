from bson import ObjectId
from pymongo import MongoClient
from datetime import datetime

from data.generation import convert_mongo_id_to_uuid


class SelectMongo:
    @staticmethod
    def groupReservationPerDevice(db):
        return db.reservations.aggregate([
            {
                "$lookup": {
                    "from": "device",
                    "localField": "id_device",
                    "foreignField": "_id",
                    "as": "device_info"
                }
            },
            {
                "$unwind": "$device_info"
            },
            {
                "$lookup": {
                    "from": "dormitory",
                    "localField": "device_info.id_dorm",
                    "foreignField": "_id",
                    "as": "dormitory_info"
                }
            },
            {
                "$unwind": "$dormitory_info"
            },
            {
                "$group": {
                    "_id": "$device_info._id",
                    "total_reservations": {"$sum": 1},
                    "dorm_name": {"$first": "$dormitory_info.dorm_name"}
                }
            },
            {
                "$sort": {
                    "total_reservations": -1
                }
            }
        ])

    @staticmethod
    def countAvarageReservationTimePerDevice(db):
        return db.reservations.aggregate([
            {
                "$lookup": {
                    "from": "device",
                    "localField": "id_device",
                    "foreignField": "_id",
                    "as": "device_info"
                }
            },
            {
                "$unwind": "$device_info"
            },
            {
                "$addFields": {
                    "reservation_duration": {
                        "$divide": [
                            {"$subtract": ["$end_date", "$start_date"]},
                            1000 * 60 * 60
                        ]
                    }
                }
            },
            {
                "$group": {
                    "_id": "$id_device",
                    "name_device": {"$first": "$device_info.name_device"},
                    "id_device": {"$first": "$device_info._id"},
                    "average_duration": {"$avg": "$reservation_duration"}
                }
            },
            {
                "$lookup": {
                    "from": "device",
                    "localField": "id_device",
                    "foreignField": "_id",
                    "as": "device_details"
                }
            },
            {
                "$unwind": "$device_details"
            },
            {

                "$sort": {
                    "average_duration": -1
                }
            }
        ])

    # nie wiem czy nie do zmiany
    @staticmethod
    def count_number_of_admins(db):
        return db.app_user.aggregate([
            {
                "$match": {
                    "authority": "ADMIN"
                }
            },
                {
                    "$lookup": {
                        "from": "dormitory",
                        "localField": "id_dorm",
                        "foreignField": "_id",
                        "as": "dorm_info"
                    }
                },
                {
                    "$unwind": "$dorm_info"
                },
                {
                    "$group": {
                        "_id": {
                            "id_dorm": "$dorm_info._id",
                            "dorm_name": "$dorm_info.dorm_name"
                        },
                        "admin_count": { "$sum": 1 }
                    }
                },
                {
                    "$sort": {
                        "admin_count": -1
                    }
                }
            ])


    @staticmethod
    def find_problem_by_id(db, id_problem):
        return db.problems.find({
            "_id": ObjectId(id_problem),
        }
        )

    @staticmethod
    def find_dormitory_messages(db, dormitoryId):
        return db.message.aggregate([
            {
                "$lookup": {
                    "from": "app_user",
                    "localField": "id_user",
                    "foreignField": "_id",
                    "as": "user_info"
                }
            },
            {
                "$unwind": "$user_info"
            },
            {
                "$match": {
                    "user_info.id_dorm": ObjectId(dormitoryId)
                }
            },
            {
                "$lookup": {
                    "from": "dormitory",
                    "localField": "user_info.id_dorm",
                    "foreignField": "_id",
                    "as": "dorm_info"
                }
            },
            {
                "$unwind": "$dorm_info"
            },
            {
                "$project": {
                    "_id": 0,
                    "message": 1,
                    "mess_date": 1,
                    "dorm_name": "$dorm_info.dorm_name"
                }
            },
            {
                "$sort": {
                    "mess_date": 1
                }
            }
        ])

    @staticmethod
    def find_reservations_earlier_then_concrete_data(db):
        date_limit = datetime(2024, 9, 1)
        return db.problems.count_documents({
            "problem_date": {"$lt": date_limit}
        })

    @staticmethod
    def find_users(db):
        return db.app_user.find()

    @staticmethod
    def count_problems_by_status(db):
        return db.problems.aggregate([
            {
                "$group": {
                    "_id": "$status",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {
                    "count": -1
                }
            }
        ])

    @staticmethod
    def group_messages_per_dormitory(db):
        return db.message.aggregate([
            {
                "$lookup": {
                    "from": "app_user",
                    "localField": "id_user",
                    "foreignField": "_id",
                    "as": "user_info"
                }
            },
            {
                "$unwind": "$user_info"
            },
            {
                "$lookup": {
                    "from": "dormitory",
                    "localField": "user_info.id_dorm",
                    "foreignField": "_id",
                    "as": "dorm_info"
                }
            },
            {
                "$unwind": "$dorm_info"
            },
            {
                "$group": {
                    "_id": "$dorm_info._id",
                    "message_count": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "dorm_id": "$_id",
                    "message_count": 1
                }
            },
            {
                "$sort": {
                    "message_count": -1
                }
            }
        ])


if __name__ == "__main__":
    client = MongoClient("mongodb://admin:password@localhost:27017/")
    db = client["dormitory_management_system"]
    # dziwne bo maks 17
    response = SelectMongo.groupReservationPerDevice(db)
    print( len(list(response)))
    response = SelectMongo.countAvarageReservationTimePerDevice(db)
    print( len(list(response)))
    response = SelectMongo.count_number_of_admins(db)
    print( len(list(response)))
    response = SelectMongo.find_problem_by_id(db, '674f8f8e8872d55fbc96ed06')
    print(list(response))
    response =  SelectMongo.find_dormitory_messages(db, '674f8f8b8872d55fbc96d8b9')
    print(len(list(response)))
    response =  SelectMongo.find_reservations_earlier_then_concrete_data(db)
    print(response)
    response = SelectMongo.find_users(db)
    print(len(list(response)))
    response = SelectMongo.count_problems_by_status(db)
    print(list(response))
    response =SelectMongo.group_messages_per_dormitory(db)
    print(len(list(response)))
