import time

class DeleteMongo:
    @staticmethod
    def delete_many_not_in_batch(db, dormitories):
        start = time.time()
        for dormitory in dormitories:
            db.dormitory.delete_one({'dorm_name': dormitory})
        elapsed = time.time() - start
        return elapsed

    @staticmethod
    def delete_many_in_batch(db, dormitories):
        start = time.time()
        db.dormitory.delete_many({'dorm_name': {'$in': dormitories}})
        elapsed = time.time() - start
        return elapsed

    @staticmethod
    def delete_one(db, dormitory):
        start = time.time()
        db.dormitory.delete_one({'dorm_name': dormitory})
        elapsed = time.time() - start
        return elapsed