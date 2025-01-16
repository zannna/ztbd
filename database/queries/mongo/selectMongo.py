import time
from bson import ObjectId
from datetime import datetime, date

class SelectMongo:
    @staticmethod
    def group_reservation_per_device(db):
        start = time.time()
        result = db.reservations.aggregate([
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
        elapsed = time.time() - start
        return result, elapsed

    @staticmethod
    def count_average_reservation_time_per_device(db):
        start = time.time()
        result = db.reservations.aggregate([
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
        elapsed = time.time() - start
        return result, elapsed

    @staticmethod
    def count_number_of_admins(db):
        start = time.time()
        result = db.app_user.aggregate([
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
        elapsed = time.time() - start
        return result, elapsed


    @staticmethod
    def find_problem_by_id(db, id_problem):
        start = time.time()
        result = db.problems.find({
            "_id": ObjectId(id_problem),
        }
        )
        elapsed = time.time() - start
        return result, elapsed

    @staticmethod
    def find_dormitory_messages(db, dormitory_id):
        start = time.time()
        result = db.message.aggregate([
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
                    "user_info.id_dorm": ObjectId(dormitory_id)
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
        elapsed = time.time() - start
        return result, elapsed

    @staticmethod
    def count_problems_earlier_then_concrete_date(db):
        current_date = date.today()
        date_limit = datetime(current_date.year, 9, 1)
        start = time.time()
        result = db.problems.count_documents({
            "problem_date": {"$lt": date_limit}
        })
        elapsed = time.time() - start
        return result, elapsed

    @staticmethod
    def find_users(db):
        start = time.time()
        result = db.app_user.find()
        elapsed = time.time() - start
        return result, elapsed

    @staticmethod
    def count_problems_by_status(db):
        start = time.time()
        result = db.problems.aggregate([
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
        elapsed = time.time() - start
        return result, elapsed

    @staticmethod
    def group_messages_per_dormitory(db):
        start = time.time()
        result = db.message.aggregate([
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
        elapsed = time.time() - start
        return result, elapsed