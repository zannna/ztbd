from math import floor

import random
import time

from tasks.task import Task
from data.stats_collector import StatsCollector
from utils.commons import PROGRESS_BAR_LENGTH, INSERT_MSG, INSERT_DORM, DORMITORY_MULT
from utils.logger import Logger
from datetime import timedelta
from database.db_utils import DbUtils
from faker import Faker


class Generator(Task):
    def __init__(self, elements : int, batch_size : int):
        super().__init__()
        self.operation = "INSERT"
        self.elements = elements
        self.batch_size = batch_size
        self.mongo_stats = StatsCollector("mongo", self.operation, self.elements, batch_size)
        self.mysql_stats = StatsCollector("mysql", self.operation, self.elements, batch_size)
        self.messages = INSERT_MSG
        self.progress_add = int(floor(PROGRESS_BAR_LENGTH / len(self.messages)))

    def run(self):
        if not self.connected:
            self.connect()
        self.generate_files()

    def generate_files(self):
        self.reset()
        batches = self.elements // self.batch_size
        Logger.log("INFO", f"Performing INSERT operations ({self.elements} records in {batches} batches)")
        fake = Faker()

        DbUtils.clear_database_mongo(self.client)
        Logger.console("[INFO] Mongo database created")

        DbUtils.clear_database(self.connection)
        DbUtils.create_mysql_database(self.connection)
        Logger.console("[INFO] MySQL database created")

        Logger.console("[INFO] Start generating data...")
        authorities = self.create_authorities()
        self.messages = INSERT_DORM
        self.progress_bar()
        dormitories_with_uuid, mysql_time_tmp, mongo_time_tmp = self.insert_dormitories()
        self.progress_bar(3)
        self.mysql_stats.add_stats(mysql_time_tmp, self.batch_size * 2)
        self.mongo_stats.add_stats(mongo_time_tmp, self.batch_size * 2)
        self.messages = INSERT_MSG
        self.progress_add = int(floor(PROGRESS_BAR_LENGTH / len(self.messages)))

        i = 0
        for batch_start in range(0, self.elements, self.batch_size):
            self.index = 0
            self.progress = 0
            i += 1
            Logger.console(f"[INFO] Generating data ({i}/{batches})")
            user_ids, mysql_time_tmp, mongo_time_tmp = self.create_users(fake, dormitories_with_uuid, authorities)
            self.mysql_stats.add_stats(mysql_time_tmp, self.batch_size * 2, 2, 2)
            self.mongo_stats.add_stats(mongo_time_tmp, self.batch_size)

            device_ids, mysql_time_tmp, mongo_time_tmp = self.create_devices(dormitories_with_uuid)
            self.mysql_stats.add_stats(mysql_time_tmp, self.batch_size)
            self.mongo_stats.add_stats(mongo_time_tmp, self.batch_size)

            mysql_time_tmp, mongo_time_tmp = self.insert_problems(fake, user_ids, dormitories_with_uuid)
            self.mysql_stats.add_stats(mysql_time_tmp, self.batch_size)
            self.mongo_stats.add_stats(mongo_time_tmp, self.batch_size)

            mysql_time_tmp, mongo_time_tmp = self.insert_messages(fake, user_ids)
            self.mysql_stats.add_stats(mysql_time_tmp, self.batch_size)
            self.mongo_stats.add_stats(mongo_time_tmp, self.batch_size)

            mysql_time_tmp, mongo_time_tmp = self.insert_reservations(fake, user_ids, device_ids)
            self.mysql_stats.add_stats(mysql_time_tmp, self.batch_size)
            self.mongo_stats.add_stats(mongo_time_tmp, self.batch_size)
            self.progress_bar(3)

        cursor = self.connection.cursor()
        cursor.close()
        self.connection.close()

        Logger.console("[INFO] Data generated")
        Logger.log("INFO", f"MySQL - data generated in {self.mysql_stats.total_time:.2f} seconds.")
        Logger.log("INFO", f"MongoDb - data generated in {self.mongo_stats.total_time:.2f} seconds.")
        Logger.save_stats(self.mysql_stats)
        Logger.save_stats(self.mongo_stats)


    def create_authorities(self):
        authorities = [{"authority": f"{role}"} for role in ["USER", "ADMIN", "MANAGER"]]

        for authority in authorities:
            authority["id_auth"] = DbUtils.generate_uuid()

        cursor = self.connection.cursor()
        cursor.executemany(
            "INSERT INTO authority (id_auth, authority) VALUES (%s, %s)",
            [(a["id_auth"], a["authority"]) for a in authorities]
        )
        self.connection.commit()
        return authorities


    def insert_dormitories(self):
        dormitories = []
        dormitory_collection = self.database["dormitory"]
        batch_size = self.batch_size * DORMITORY_MULT
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
        cursor = self.connection.cursor()
        cursor.executemany(
            "INSERT INTO dormitory (id_dorm, dorm_name) VALUES (%s, %s)",
            mysql_data
        )
        self.connection.commit()
        mysql_time = time.time() - start_time

        return dormitories, mysql_time, mongo_time


    def create_users(self, fake, dormitories_with_uuid, authorities):
        self.progress_bar()
        users = []
        app_user = self.database["app_user"]
        for _ in range(self.batch_size):
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

        self.progress_bar(1)
        cursor = self.connection.cursor()
        start = time.time()
        cursor.executemany(
            """
            INSERT INTO app_user (id_user, email, first_name, surname, password, room, university, id_dorm)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, mysql_data
        )
        self.connection.commit()

        cursor.executemany(
            """
            INSERT INTO users_authorities (id_user, id_auth)
            VALUES (%s, %s)
            """, mysql_data1
        )
        self.connection.commit()
        mysql_time = time.time() - start

        return list(result.inserted_ids), mysql_time, mongo_time


    def create_devices(self, dormitories_with_uuid):
        self.progress_bar(2)
        devices = []
        device_db = self.database["device"]
        for _ in range(self.batch_size):
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

        self.progress_bar(1)
        mysql_data = [(DbUtils.convert_mongo_id_to_uuid(id), d["name_device"], d["number"], d["work"],
                          DbUtils.convert_mongo_id_to_uuid(d["id_dorm"]))
                         for id, d in zip(device_ids, devices)]
        cursor = self.connection.cursor()
        start = time.time()
        cursor.executemany(
            """
            INSERT INTO device (id_device, name_device, number, work, id_dorm)
            VALUES (%s, %s, %s, %s, %s)
            """, mysql_data
        )
        self.connection.commit()
        mysql_time = time.time() - start

        return device_ids, mysql_time, mongo_time


    def insert_problems(self, fake, user_ids, dormitories_with_uuid):
        self.progress_bar(2)
        problems_data = []
        problems = self.database["problems"]
        for _ in range(self.batch_size):
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

        self.progress_bar(1)
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
        cursor = self.connection.cursor()
        start = time.time()
        cursor.executemany(
            """
            INSERT INTO problems (id_problem, description, problem_date, status, id_user, id_dorm)
            VALUES (%s, %s, %s, %s, %s, %s)
            """, mysql_data
        )
        self.connection.commit()
        mysql_time = time.time() - start

        return mysql_time, mongo_time


    def insert_messages(self, fake, user_ids):
        self.progress_bar(2)
        messages = []
        message = self.database["message"]
        for _ in range(self.batch_size):
            message_item = {
                "mess_date": fake.date_time_this_year(),
                "message": fake.text(max_nb_chars=200),
                "id_user": random.choice(user_ids)
            }
            messages.append(message_item)

        start = time.time()
        message_ids = message.insert_many(messages).inserted_ids
        mongo_time = time.time() - start

        self.progress_bar(1)
        mysql_data = [(DbUtils.generate_uuid(), m["mess_date"], m["message"], DbUtils.convert_mongo_id_to_uuid(m["id_user"]))
                        for id, m in zip(message_ids, messages)]
        cursor = self.connection.cursor()
        start = time.time()
        cursor.executemany(
            """
            INSERT INTO message (id_mess, mess_date, message, id_user)
            VALUES (%s, %s, %s, %s)
            """, mysql_data
        )
        self.connection.commit()
        mysql_time = time.time() - start

        return mysql_time, mongo_time


    def insert_reservations(self, fake, user_ids, device_ids):
        self.progress_bar(2)
        reservations = self.database["reservations"]
        reservations_data = []
        for _ in range(self.batch_size):
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

        self.progress_bar(1)
        mysql_data = [(DbUtils.generate_uuid(), r["start_date"], r["end_date"], DbUtils.convert_mongo_id_to_uuid(r["id_user"]),
                          DbUtils.convert_mongo_id_to_uuid(r["id_device"]))
                         for id, r in zip(reservations_ids, reservations_data)]
        cursor = self.connection.cursor()
        start = time.time()
        cursor.executemany(
            """
            INSERT INTO reservations (id_res, start_date, end_date, id_user, id_device)
            VALUES (%s, %s, %s, %s, %s)
            """, mysql_data
        )
        self.connection.commit()
        mysql_time = time.time() - start

        return mysql_time, mongo_time