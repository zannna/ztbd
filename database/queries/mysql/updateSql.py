import mysql.connector


def run_query(connection, query):
    cursor = connection.cursor(dictionary=True)
    cursor.execute(query)
    connection.commit()
    return {"affected_rows": cursor.rowcount}


def update_password_and_other_fields_when_user_is_admin(conn):
    return run_query(conn,
                     """
                     UPDATE app_user
                    JOIN users_authorities ON app_user.id_user = users_authorities.id_user
                    JOIN authority ON users_authorities.id_auth = authority.id_auth
                    SET 
                        room = room + 1,
                        password = CONCAT(password, '123'),
                        surname = 'admin'
                    WHERE authority.authority = 'ADMIN';
                     """)


def update_reservation_time(conn):
    return run_query(conn,
                     """
                         UPDATE reservations
                         SET 
                             start_date = DATE_ADD(start_date, INTERVAL 1 HOUR),
                             end_date = DATE_ADD(end_date, INTERVAL 1 HOUR);
                     """
                     )


def update_status_when_problem_is_older_than(conn):
    return run_query(conn,
                     """
                            UPDATE problems
                            SET status = 0
                            WHERE problem_date < '2024-04-15';
                     """)

def update_one_message(conn):
    return run_query(conn,
                     """
                            UPDATE message
                            SET message = 'new message'
                            LIMIT 1;
                     """)

if __name__ == "__main__":
    conn = mysql.connector.connect(
        host="localhost",
        user="user",
        password="password",
        database="dormitory_management_system"
    )
    result = update_password_and_other_fields_when_user_is_admin(conn)
    print(result["affected_rows"])
    result = update_reservation_time(conn)
    print(result["affected_rows"])
    result = update_status_when_problem_is_older_than(conn)
    print(result["affected_rows"])
    result = update_one_message(conn)
    print(result["affected_rows"])
