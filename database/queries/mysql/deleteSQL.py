import mysql


def delete_many_not_in_batch(conn, dorm_names):
    cursor = conn.cursor()
    for dorm_name in dorm_names:
        cursor.execute("DELETE FROM dormitory WHERE dorm_name = ?", (dorm_name,))
    conn.commit()


def delete_many_in_batch(conn, placeholders):
    cursor = conn.cursor()
    query = f"DELETE FROM dormitory WHERE dorm_name IN ({placeholders})"
    cursor.execute(query, dorm_names)
    conn.commit()


def delete_one(conn, dorm_name):
    cursor = conn.cursor()
    cursor.execute("DELETE FROM dormitory WHERE dorm_name = ?", (dorm_name,))
    conn.commit()

if __name__ == "__main__":
    conn = mysql.connector.connect(
        host="localhost",
        user="user",
        password="password",
        database="dormitory_management_system"
    )
    dorm_names = [f"Dormitory {i}" for i in range(1, 10)]
    delete_many_not_in_batch(conn, dorm_names)

    dorm_names = [f"Dormitory {i}" for i in range(10, 20)]
    placeholders = ', '.join(['?'] * len(dorm_names))
    delete_many_in_batch(conn, placeholders)

    dorm_name = f"Dormitory "
    delete_one(conn, dorm_name)