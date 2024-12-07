import uuid
from pymongo import MongoClient
from faker import Faker
import random
from datetime import timedelta
import mysql.connector
from database.dbcreation import create_postgres_database
from database.clear import clear_database
from database.clear import clear_databaseMongo

namespace = uuid.UUID('12345678-1234-5678-1234-567812345678')
def generate_files(num_elements, batch_size):
    fake = Faker()

    client = MongoClient("mongodb://admin:password@localhost:27017/")
    clear_databaseMongo(client)
    db = client["dormitory_management_system"]

    dormitory = db["dormitory"]
    app_user = db["app_user"]
    device_db = db["device"]
    problems = db["problems"]
    message = db["message"]
    reservations = db["reservations"]

    mysql_conn = mysql.connector.connect(
        host="localhost",
        user="user",
        password="password",
        database="dormitory_management_system"
    )
    clear_database(mysql_conn)
    create_postgres_database(mysql_conn)

    cursor = mysql_conn.cursor()


    authorities = create_authorities(cursor, mysql_conn)
    dormitories_with_uuid = insert_dormitories(cursor, mysql_conn, batch_size*2, dormitory)

    for batch_start in range(0, num_elements, batch_size):
        user_ids = create_users(cursor, mysql_conn, batch_size, fake, dormitories_with_uuid, authorities, app_user)
        device_ids = create_devices(cursor, mysql_conn, batch_size, dormitories_with_uuid, device_db)
        insert_problems(cursor, mysql_conn, fake, problems, batch_size, user_ids, dormitories_with_uuid)
        insert_messages(cursor, mysql_conn, fake, message, batch_size, user_ids)
        insert_reservations(cursor, mysql_conn, fake, reservations, batch_size, user_ids, device_ids)

    cursor.close()
    mysql_conn.close()


def create_authorities(cursor, mysql_conn):
    authorities = [{"authority": f"{role}"} for role in ["USER", "ADMIN", "MANAGER"]]

    for authority in authorities:
        authority["id_auth"] = str(uuid.uuid4())

    cursor.executemany(
        "INSERT INTO authority (id_auth, authority) VALUES (%s, %s)",
        [(a["id_auth"], a["authority"]) for a in authorities]
    )
    mysql_conn.commit()
    return  authorities

def insert_dormitories(cursor, mysql_conn, batch_size, dormitory_collection):
    dormitories = []

    for i in range(1, batch_size):
        dorm = {
            "dorm_name": f"Dormitory {i}",
        }
        dormitories.append(dorm)

    dormitory_collection.insert_many(dormitories)
    dormitories_ids = [convert_mongo_id_to_uuid(dormitory["_id"]) for dormitory in dormitories]

    cursor.executemany(
        "INSERT INTO dormitory (id_dorm, dorm_name) VALUES (%s, %s)",
        [(id, d["dorm_name"]) for id, d in zip(dormitories_ids, dormitories)]
    )
    mysql_conn.commit()

    return dormitories

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


    result = app_user.insert_many(users)

    cursor.executemany(
        """
        INSERT INTO app_user (id_user, email, first_name, surname, password, room, university, id_dorm)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        [( convert_mongo_id_to_uuid(u["_id"]), u["email"], u["first_name"], u["surname"], u["password"], u["room"], u["university"],
          convert_mongo_id_to_uuid(u["id_dorm"]))
         for u in users]
    )
    mysql_conn.commit()

    cursor.executemany(
        """
        INSERT INTO users_authorities (id_user, id_auth)
        VALUES (%s, %s)
        """,
        [( convert_mongo_id_to_uuid(u["_id"]), next(a["id_auth"] for a in authorities if a["authority"] == u["authority"])) for u in users]
    )
    mysql_conn.commit()
    return list(result.inserted_ids)


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

    device_ids = device_db.insert_many(devices).inserted_ids

    cursor.executemany(
        """
        INSERT INTO device (id_device, name_device, number, work, id_dorm)
        VALUES (%s, %s, %s, %s, %s)
        """,
        [(convert_mongo_id_to_uuid(id), d["name_device"], d["number"], d["work"], convert_mongo_id_to_uuid(d["id_dorm"])) for id, d in zip(device_ids, devices)]
    )
    mysql_conn.commit()

    return device_ids


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

    problem_ids = problems.insert_many(problems_data).inserted_ids

    cursor.executemany(
        """
        INSERT INTO problems (id_problem, description, problem_date, status, id_user, id_dorm)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        [
            (
                convert_mongo_id_to_uuid(id),  # Convert MongoDB ObjectId to UUID
                p["description"],
                p["problem_date"],
                p["status"],
                convert_mongo_id_to_uuid(p["id_user"]),
                convert_mongo_id_to_uuid(p["id_dorm"])
            )
            for id, p in zip(problem_ids, problems_data)
        ]
    )
    mysql_conn.commit()


def insert_messages(cursor, mysql_conn, fake, message, batch_size, user_ids):
    messages = []
    for _ in range(batch_size):
        message_item = {
            "mess_date": fake.date_time_this_year(),
            "message": fake.text(max_nb_chars=200),
            "id_user": random.choice(user_ids)
        }
        messages.append(message_item)

    message_ids = message.insert_many(messages).inserted_ids

    cursor.executemany(
        """
        INSERT INTO message (id_mess, mess_date, message, id_user)
        VALUES (%s, %s, %s, %s)
        """,
        [(str(uuid.uuid4()), m["mess_date"], m["message"], convert_mongo_id_to_uuid(m["id_user"])) for id, m in zip(message_ids, messages)]
    )
    mysql_conn.commit()


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

    reservations_ids = reservations.insert_many(reservations_data).inserted_ids

    cursor.executemany(
        """
        INSERT INTO reservations (id_res, start_date, end_date, id_user, id_device)
        VALUES (%s, %s, %s, %s, %s)
        """,
        [(str(uuid.uuid4()), r["start_date"], r["end_date"], convert_mongo_id_to_uuid(r["id_user"]), convert_mongo_id_to_uuid(r["id_device"]))
         for id, r in zip(reservations_ids, reservations_data)]
    )
    mysql_conn.commit()

def convert_mongo_id_to_uuid(id):
    return str(uuid.uuid5(namespace, str(id)))

if __name__ == "__main__":
    generate_files(8000, 2000)
