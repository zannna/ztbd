from datetime import datetime
from pymongo import MongoClient


def update_password_and_other_fields_when_user_is_admin(db):
    return db.app_user.update_many(
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


def update_reservation_time(db):
    return db.reservations.update_many(
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


def update_status_when_problem_is_older_than(db):
    return db.problems.update_many(
        {"problem_date": {"$lt": datetime(2024, 4, 15)}},
        {"$set": {"status": 0}}
    )


def update_one_message(db):
    return db.message.update_one({},{"$set": {"message": "new message"}})


if __name__ == "__main__":
    client = MongoClient("mongodb://admin:password@localhost:27017/")
    db = client["dormitory_management_system"]

    result = update_one_message(db)
    print(result.matched_count, result.modified_count)

    result = update_reservation_time(db)
    print(result.matched_count, result.modified_count)

    result = update_status_when_problem_is_older_than(db)
    print(result.matched_count, result.modified_count)

    result = update_one_message(db)
    print(result.matched_count, result.modified_count)
