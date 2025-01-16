import time
from datetime import datetime
# from pymongo import MongoClient

class UpdateMongo:
    @staticmethod
    def update_password_and_other_fields_when_user_is_admin(db):
        start = time.time()
        result =  db.app_user.update_many(
            {"authority": "ADMIN"},
            [
                {
                    "$set": {
                        "surname": "admin",
                        "password": {"$concat": ["$password", "123"]},
                        "room": { "$add": ["$room", 1]}
                    }
                }
            ]
        )
        elapsed_time = time.time() - start
        return result, elapsed_time

    @staticmethod
    def update_reservation_time(db):
        start = time.time()
        result = db.reservations.update_many(
            {},
            [
                {
                    "$set": {
                        "start_date": {"$dateAdd": {"startDate": "$start_date", "unit": "hour", "amount": 1}},
                        "end_date": {"$dateAdd": {"startDate": "$end_date", "unit": "hour", "amount": 1}}
                    }
                }
            ]
        )
        elapsed_time = time.time() - start
        return result, elapsed_time

    @staticmethod
    def update_status_when_problem_is_older_than(db):
        year_tmp = datetime.now().year
        start = time.time()
        result = db.problems.update_many(
            {"problem_date": {"$lt": datetime(year_tmp, 4, 15)}},
            {"$set": {"status": 0}}
        )
        elapsed_time = time.time() - start
        return result, elapsed_time

    @staticmethod
    def update_one_message(db):
        start = time.time()
        result = db.message.update_one({},{"$set": {"message": "new message"}})
        elapsed_time = time.time() - start
        return result, elapsed_time