import mysql.connector

class DeleteSQL:
    @staticmethod
    def delete_many_not_in_batch(connection, dormitories):
        cursor = connection.cursor()
        for dormitory in dormitories:
            cursor.execute("DELETE FROM dormitory WHERE dorm_name = ?", (dormitory,))
        connection.commit()

    @staticmethod
    def delete_many_in_batch(connection, dormitories):
        cursor = connection.cursor()
        query = f"DELETE FROM dormitory WHERE dorm_name IN ({dormitories})"
        cursor.execute(query, dorm_names)
        connection.commit()

    @staticmethod
    def delete_one(connection, dormitory):
        cursor = connection.cursor()
        cursor.execute("DELETE FROM dormitory WHERE dorm_name = ?", (dormitory,))
        connection.commit()

if __name__ == "__main__":
    conn = mysql.connector.connect(
        host="localhost",
        user="user",
        password="password",
        database="dormitory_management_system"
    )
    dorm_names = [f"Dormitory {i}" for i in range(1, 10)]
    DeleteSQL.delete_many_not_in_batch(conn, dorm_names)

    dorm_names = [f"Dormitory {i}" for i in range(10, 20)]
    placeholders = ', '.join(['?'] * len(dorm_names))
    DeleteSQL.delete_many_in_batch(conn, placeholders)

    dorm_name = f"Dormitory 3979"
    DeleteSQL.delete_one(conn, dorm_name)