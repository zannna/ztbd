import time

import mysql.connector

class DeleteSQL:
    @staticmethod
    def delete_many_not_in_batch(connection, dormitories):
        cursor = connection.cursor()
        start = time.time()
        for dorm in dormitories:
            cursor.execute("DELETE FROM dormitory WHERE dorm_name = %s", (dorm,))
        connection.commit()
        elapsed = time.time() - start
        return elapsed

    @staticmethod
    def delete_many_in_batch(connection, dormitories, placeholders):
        cursor = connection.cursor()
        start = time.time()
        query = f"DELETE FROM dormitory WHERE dorm_name IN ({placeholders})"
        cursor.execute(query, dormitories)
        connection.commit()
        elapsed = time.time() - start
        return elapsed

    @staticmethod
    def delete_one(connection, dorm):
        cursor = connection.cursor()
        start = time.time()
        cursor.execute("DELETE FROM dormitory WHERE dorm_name = %s", (dorm,))
        connection.commit()
        elapsed = time.time() - start
        return elapsed