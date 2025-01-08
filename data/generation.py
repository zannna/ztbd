import mysql.connector
import random
import time

from data.stats_collector import StatsCollector
from utils.commons import DB_NAME, DB_HOST, DB_USER
from utils.logger import Logger
from datetime import timedelta
from database.db_utils import DbUtils
from faker import Faker
from pymongo import MongoClient


class Generator:
    @staticmethod
    def generate_files(num_elements, batch_size):
        batches = num_elements // batch_size
        Logger.log("INFO", f"Generating data ({num_elements} records in {batches} batches)")
        fake = Faker()

        Logger.log("INFO", "Create clear Mongo database")
        client = MongoClient("mongodb://admin:password@localhost:27017/")
        DbUtils.clear_database_mongo(client)
        db = client[DB_NAME]
        Logger.log("INFO", "Mongo database created")

        dormitory = db["dormitory"]
        app_user = db["app_user"]
        device_db = db["device"]
        problems = db["problems"]
        message = db["message"]
        reservations = db["reservations"]

        mysql_conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
        Logger.log("INFO", "Create clear MySQL database")
        DbUtils.clear_database(mysql_conn)
        DbUtils.create_mysql_database(mysql_conn)
        Logger.log("INFO", "MySQL database created")

        cursor = mysql_conn.cursor()

        # Do zliczania
        mongo_stats = StatsCollector("mongo", "INSERT", num_elements, batch_size)
        mysql_stats = StatsCollector("mysql", "INSERT", num_elements, batch_size)
        mysql_time_tmp = 0
        mongo_time_tmp = 0

        Logger.log("INFO", f"Start generating data...")
        start = time.time()
        authorities = Generator.create_authorities(cursor, mysql_conn)
        dormitories_with_uuid, mysql_time_tmp, mongo_time_tmp = Generator.insert_dormitories(cursor,
                                                                                             mysql_conn,
                                                                                             batch_size*2,
                                                                                             dormitory)

        mysql_stats.add_stats(mysql_time_tmp, batch_size * 2)
        mongo_stats.add_stats(mongo_time_tmp, batch_size * 2)

        i = 0
        for batch_start in range(0, num_elements, batch_size):
            i += 1
            Logger.log("INFO", f"Generating data ({i}/{batches})")
            user_ids, mysql_time_tmp, mongo_time_tmp = Generator.create_users(cursor, mysql_conn, batch_size,
                                                                              fake, dormitories_with_uuid,
                                                                              authorities, app_user)
            mysql_stats.add_stats(mysql_time_tmp, batch_size * 2, 2, 2)
            mongo_stats.add_stats(mongo_time_tmp, batch_size)

            device_ids, mysql_time_tmp, mongo_time_tmp = Generator.create_devices(cursor, mysql_conn,
                                                                                    batch_size,
                                                                                    dormitories_with_uuid,
                                                                                    device_db)
            mysql_stats.add_stats(mysql_time_tmp, batch_size)
            mongo_stats.add_stats(mongo_time_tmp, batch_size)

            mysql_time_tmp, mongo_time_tmp = Generator.insert_problems(cursor, mysql_conn, fake, problems,
                                                                         batch_size, user_ids,
                                                                         dormitories_with_uuid)
            mysql_stats.add_stats(mysql_time_tmp, batch_size)
            mongo_stats.add_stats(mongo_time_tmp, batch_size)

            mysql_time_tmp, mongo_time_tmp = Generator.insert_messages(cursor, mysql_conn, fake, message,
                                                                         batch_size, user_ids)
            mysql_stats.add_stats(mysql_time_tmp, batch_size)
            mongo_stats.add_stats(mongo_time_tmp, batch_size)

            mysql_time_tmp, mongo_time_tmp = Generator.insert_reservations(cursor, mysql_conn, fake,
                                                                            reservations, batch_size,
                                                                            user_ids, device_ids)
            mysql_stats.add_stats(mysql_time_tmp, batch_size)
            mongo_stats.add_stats(mongo_time_tmp, batch_size)

        end = time.time() - start
        cursor.close()
        mysql_conn.close()

        Logger.log("INFO", f"Data generated in {end:.2f} seconds.")
        Logger.log("INFO", f"MySQL - data generated in {mysql_stats.total_time:.2f} seconds.")
        Logger.log("INFO", f"MongoDb - data generated in {mongo_stats.total_time:.2f} seconds.")
        Logger.save_stats(mysql_stats)
        Logger.save_stats(mongo_stats)


    @staticmethod
    def create_authorities(cursor, mysql_conn):
        authorities = [{"authority": f"{role}"} for role in ["USER", "ADMIN", "MANAGER"]]

        for authority in authorities:
            authority["id_auth"] = DbUtils.generate_uuid()

        cursor.executemany(
            "INSERT INTO authority (id_auth, authority) VALUES (%s, %s)",
            [(a["id_auth"], a["authority"]) for a in authorities]
        )
        mysql_conn.commit()
        return authorities


    @staticmethod
    def insert_dormitories(cursor, mysql_conn, batch_size, dormitory_collection):
        dormitories = []
        for i in range(1, batch_size):
            dorm = {
                "dorm_name": f"Dormitory {i}",
            }
            dormitories.append(dorm)

        start_time = time.time()
        dormitory_collection.insert_many(dormitories)
        mongo_time = time.time() - start_time
        dormitories_ids = [DbUtils.convert_mongo_id_to_uuid(dormitory["_id"]) for dormitory in dormitories]

        mysql_data = [(id, d["dorm_name"]) for id, d in zip(dormitories_ids, dormitories)]
        start_time = time.time()
        cursor.executemany(
            "INSERT INTO dormitory (id_dorm, dorm_name) VALUES (%s, %s)",
            mysql_data
        )
        mysql_conn.commit()
        mysql_time = time.time() - start_time

        return dormitories, mysql_time, mongo_time


    @staticmethod
    def create_users(cursor, mysql_conn, batch_size, fake, dormitories_with_uuid, authorities, app_user):
        users = []
        for _ in range(batch_size):
            authority = random.choice(authorities)
            user = {
                "email": fake.email(),
                "first_name": fake.first_name(),
                "surname": fake.last_name(),
                "password": fake.password(),
                "room": random.randint(1, 500),
                "university": fake.company(),
                "id_dorm": random.choice(dormitories_with_uuid)["_id"],
                "authority": authority["authority"]
            }
            users.append(user)

        start = time.time()
        result = app_user.insert_many(users)
        mongo_time = time.time() - start

        mysql_data = [(DbUtils.convert_mongo_id_to_uuid(u["_id"]), u["email"], u["first_name"], u["surname"],
                          u["password"], u["room"], u["university"],
                          DbUtils.convert_mongo_id_to_uuid(u["id_dorm"]))
                        for u in users]
        mysql_data1 = [(DbUtils.convert_mongo_id_to_uuid(u["_id"]), next(a["id_auth"]
                        for a in authorities if a["authority"] == u["authority"])) for u in users]

        start_time = time.time()
        cursor.executemany(
            """
            INSERT INTO app_user (id_user, email, first_name, surname, password, room, university, id_dorm)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, mysql_data
        )
        mysql_conn.commit()

        cursor.executemany(
            """
            INSERT INTO users_authorities (id_user, id_auth)
            VALUES (%s, %s)
            """, mysql_data1
        )
        mysql_conn.commit()
        mysql_time = time.time() - start

        return list(result.inserted_ids), mysql_time, mongo_time


    @staticmethod
    def create_devices(cursor, mysql_conn, batch_size, dormitories_with_uuid, device_db):
        devices = []
        for _ in range(batch_size):
            device = {
                "name_device": f"Device {_}",
                "number": random.randint(1, 100),
                "work": random.choice([True, False]),
                "id_dorm": random.choice(dormitories_with_uuid)["_id"]
            }
            devices.append(device)

        start = time.time()
        device_ids = device_db.insert_many(devices).inserted_ids
        mongo_time = time.time() - start

        mysql_data = [(DbUtils.convert_mongo_id_to_uuid(id), d["name_device"], d["number"], d["work"],
                          DbUtils.convert_mongo_id_to_uuid(d["id_dorm"]))
                         for id, d in zip(device_ids, devices)]
        start = time.time()
        cursor.executemany(
            """
            INSERT INTO device (id_device, name_device, number, work, id_dorm)
            VALUES (%s, %s, %s, %s, %s)
            """, mysql_data
        )
        mysql_conn.commit()
        mysql_time = time.time() - start

        return device_ids, mysql_time, mongo_time


    @staticmethod
    def insert_problems(cursor, mysql_conn, fake, problems, batch_size, user_ids, dormitories_with_uuid):
        problems_data = []
        for _ in range(batch_size):
            problem = {
                "description": fake.sentence(),
                "problem_date": fake.date_time_this_year(),
                "status": random.choice([0, 1]),
                "id_user": random.choice(user_ids),
                "id_dorm": random.choice(dormitories_with_uuid)["_id"]
            }
            problems_data.append(problem)

        start = time.time()
        problem_ids = problems.insert_many(problems_data).inserted_ids
        mongo_time = time.time() - start

        mysql_data = [
                        (
                            DbUtils.convert_mongo_id_to_uuid(id),  # Convert MongoDB ObjectId to UUID
                            p["description"],
                            p["problem_date"],
                            p["status"],
                            DbUtils.convert_mongo_id_to_uuid(p["id_user"]),
                            DbUtils.convert_mongo_id_to_uuid(p["id_dorm"])
                        )
                        for id, p in zip(problem_ids, problems_data)
                     ]
        start = time.time()
        cursor.executemany(
            """
            INSERT INTO problems (id_problem, description, problem_date, status, id_user, id_dorm)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, mysql_data
        )
        mysql_conn.commit()
        mysql_time = time.time() - start

        return mysql_time, mongo_time


    @staticmethod
    def insert_messages(cursor, mysql_conn, fake, message, batch_size, user_ids):
        messages = []
        for _ in range(batch_size):
            message_item = {
                "mess_date": fake.date_time_this_year(),
                "message": fake.text(max_nb_chars=200),
                "id_user": random.choice(user_ids)
            }
            messages.append(message_item)

        start = time.time()
        message_ids = message.insert_many(messages).inserted_ids
        mongo_time = time.time() - start

        mysql_data = [(DbUtils.generate_uuid(), m["mess_date"], m["message"], DbUtils.convert_mongo_id_to_uuid(m["id_user"]))
                        for id, m in zip(message_ids, messages)]
        start = time.time()
        cursor.executemany(
            """
            INSERT INTO message (id_mess, mess_date, message, id_user)
            VALUES (%s, %s, %s, %s)
            """, mysql_data
        )
        mysql_conn.commit()
        mysql_time = time.time() - start

        return mysql_time, mongo_time


    @staticmethod
    def insert_reservations(cursor, mysql_conn, fake, reservations, batch_size, user_ids, device_ids):
        reservations_data = []
        for _ in range(batch_size):
            start_date = fake.date_time_this_year()
            end_date = start_date + timedelta(hours=random.randint(1, 6)) + timedelta(minutes=random.randint(0, 60))
            reservation = {
                "start_date": start_date,
                "end_date": end_date,
                "id_user": random.choice(user_ids),
                "id_device": random.choice(device_ids)
            }
            reservations_data.append(reservation)

        start = time.time()
        reservations_ids = reservations.insert_many(reservations_data).inserted_ids
        mongo_time = time.time() - start

        mysql_data = [(DbUtils.generate_uuid(), r["start_date"], r["end_date"], DbUtils.convert_mongo_id_to_uuid(r["id_user"]),
                          DbUtils.convert_mongo_id_to_uuid(r["id_device"]))
                         for id, r in zip(reservations_ids, reservations_data)]
        start = time.time()
        cursor.executemany(
            """
            INSERT INTO reservations (id_res, start_date, end_date, id_user, id_device)
            VALUES (%s, %s, %s, %s, %s)
            """, mysql_data
        )
        mysql_conn.commit()
        mysql_time = time.time() - start

        return mysql_time, mongo_time