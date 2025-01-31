import time
from datetime import date

import mysql.connector


class SelectSQL:
    @staticmethod
    def run_query(connection, query):
        cursor = connection.cursor(dictionary=True)

        start = time.time()
        cursor.execute(query)
        result = cursor.fetchall()
        elapsed = time.time() - start

        return result, elapsed

    @staticmethod
    def group_reservation_per_device(connection):
        return SelectSQL.run_query(connection,
                                   """
                                     SELECT 
                                         d.name_device AS device_name,
                                         COUNT(r.id_res) AS total_reservations,
                                         dorm.dorm_name AS dorm_name
                                     FROM 
                                         reservations r
                                     JOIN 
                                         device d ON r.id_device = d.id_device
                                     JOIN 
                                         dormitory dorm ON d.id_dorm = dorm.id_dorm
                                     GROUP BY 
                                         d.id_device
                                     ORDER BY 
                                         total_reservations DESC;
                                   """)

    @staticmethod
    def count_average_reservation_time_per_device(connection):
        return SelectSQL.run_query(connection,
                                   """
                                   SELECT 
                                       d.id_device,
                                       d.name_device,
                                       AVG(TIMESTAMPDIFF(SECOND, r.start_date, r.end_date)) / 3600 AS average_duration
                                   FROM reservations r
                                   JOIN device d ON r.id_device = d.id_device
                                   GROUP BY d.id_device
                                   ORDER BY average_duration DESC;
                                   """)

    @staticmethod
    def find_problem_by_id(connection, problem_id):
        return SelectSQL.run_query(connection,
                                   f"""
                            SELECT *
                            FROM problems p
                            WHERE p.id_problem = '{problem_id}';
                            """)

    @staticmethod
    def count_number_of_admins(connection):
        return SelectSQL.run_query(connection,
                                   """
                                   SELECT 
                                       dorm.id_dorm,
                                       dorm.dorm_name,
                                       COUNT(ua.id_user) AS admin_count
                                   FROM users_authorities ua
                                   JOIN authority a ON ua.id_auth = a.id_auth
                                   JOIN app_user u ON ua.id_user = u.id_user
                                   JOIN dormitory dorm ON u.id_dorm = dorm.id_dorm
                                   WHERE a.authority = 'ADMIN'
                                   GROUP BY dorm.id_dorm
                                   ORDER BY admin_count DESC;
                                   """)

    @staticmethod
    def find_dormitory_messages(connection, dormitory_id):
        return SelectSQL.run_query(connection,
                                   f"""
                         SELECT 
                             m.message,
                             m.mess_date,
                             dorm.dorm_name
                         FROM message m
                         JOIN app_user u ON m.id_user = u.id_user
                         JOIN dormitory dorm ON u.id_dorm = dorm.id_dorm
                         WHERE dorm.id_dorm = '{dormitory_id}'
                         ORDER BY m.mess_date ASC;
                         """)

    @staticmethod
    def count_problems_earlier_then_concrete_date(connection):
        current_date = date.today()
        date_tmp = f"{current_date.year}-09-01"
        return SelectSQL.run_query(connection,
                                   f"""
                                   SELECT COUNT(*) AS problem_count
                                   FROM problems
                                   WHERE problem_date < '{date_tmp}';
                                   """)

    @staticmethod
    def find_users(connection):
        return SelectSQL.run_query(connection,
                                   """
                                   SELECT * from app_user;
                                   """)

    @staticmethod
    def count_problems_by_status(connection):
        return SelectSQL.run_query(connection,
                                   """
                                   SELECT 
                                       status,
                                       COUNT(*) AS count
                                   FROM problems
                                   GROUP BY status
                                   ORDER BY count DESC;
                                   """)

    @staticmethod
    def group_messages_per_dormitory(connection):
        return SelectSQL.run_query(connection,
                                   """
                                               SELECT 
                                                   d.dorm_name AS dorm_name,
                                                   COUNT(m.id_mess) AS message_count
                                               FROM 
                                                   message m
                                               JOIN 
                                                   app_user u ON m.id_user = u.id_user
                                               JOIN 
                                                   dormitory d ON u.id_dorm = d.id_dorm
                                               GROUP BY 
                                                   d.dorm_name
                                               ORDER BY 
                                                   message_count DESC;
                                           """
                                   )