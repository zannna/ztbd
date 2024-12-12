import mysql.connector


class SelectSql:

    @staticmethod
    def run_query(connection, query):
        cursor = connection.cursor(dictionary=True)

        cursor.execute(query)

        return cursor.fetchall()

    @staticmethod
    def groupReservationPerDevice(connection):
        return SelectSql.run_query(connection,
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
    def countAvarageReservationTimePerDevice(connection):
        return SelectSql.run_query(connection,
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
        return SelectSql.run_query(connection,
                                   f"""
                            SELECT *
                            FROM problems p
                            WHERE p.id_problem = '{problem_id}';
                            """)

    @staticmethod
    def count_number_of_admins(connection):
        return SelectSql.run_query(connection,
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
        return SelectSql.run_query(connection,
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
    def find_reservations_earlier_then_concrete_data(connection):
        return SelectSql.run_query(connection,
                                   """
                                   SELECT COUNT(*) AS problem_count
                                  FROM problems
                                  WHERE problem_date < '2024-09-01';
                                   """)

    @staticmethod
    def find_users(connection):
        return SelectSql.run_query(connection,
                                   """
                                   SELECT * from app_user;
                                   """)

    @staticmethod
    def count_problems_by_status(connection):
        return SelectSql.run_query(connection,
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
        return SelectSql.run_query(connection,
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


if __name__ == "__main__":
    conn = mysql.connector.connect(
        host="localhost",
        user="user",
        password="password",
        database="dormitory_management_system"
    )

    response = SelectSql.groupReservationPerDevice(conn)
    print(len(list(response)))
    response = SelectSql.countAvarageReservationTimePerDevice(conn)
    print(len(list(response)))
    response = SelectSql.count_number_of_admins(conn)
    print(len(list(response)))
    response = SelectSql.find_problem_by_id(conn, '000f28d2-eb2b-586a-949a-6439378f4d18')
    print(response)
    response = SelectSql.find_dormitory_messages(conn, '00059349-f566-566d-a89d-c423576285d0')
    print(len(list(response)))
    response = SelectSql.find_reservations_earlier_then_concrete_data(conn)
    print(response)
    response = SelectSql.find_users(conn)
    print(len(list(response)))
    response = SelectSql.count_problems_by_status(conn)
    print(response)
    response = SelectSql.group_messages_per_dormitory(conn)
    print(len(list(response)))
    conn.close()
