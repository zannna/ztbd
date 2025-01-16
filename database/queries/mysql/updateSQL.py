import mysql.connector
import time
from datetime import date

class UpdateSQL:
    @staticmethod
    def run_query(connection, query):
        cursor = connection.cursor(dictionary=True)
        start = time.time()
        cursor.execute(query)
        connection.commit()
        end = time.time()
        return {"affected_rows": cursor.rowcount, "total_time": end - start}

    @staticmethod
    def update_password_and_other_fields_when_user_is_admin(connection):
        return UpdateSQL.run_query(connection,
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

    @staticmethod
    def update_reservation_time(connection):
        return UpdateSQL.run_query(connection,
                         """
                             UPDATE reservations
                             SET 
                                 start_date = DATE_ADD(start_date, INTERVAL 1 HOUR),
                                 end_date = DATE_ADD(end_date, INTERVAL 1 HOUR);
                         """
                         )

    @staticmethod
    def update_status_when_problem_is_older_than(connection):
        current_date = date.today()
        date_tmp = f"{current_date.year}-04-15"
        return UpdateSQL.run_query(connection,
                         f"""
                                UPDATE problems
                                SET status = 0
                                WHERE problem_date < '{date_tmp}';
                         """)

    @staticmethod
    def update_one_message(connection):
        return UpdateSQL.run_query(connection,
                         """
                                UPDATE message
                                SET message = 'new message'
                                LIMIT 1;
                         """)