import mysql.connector
from data.generation import Generator
from data.stats_collector import StatsCollector
from database.queries.mongo.deleteMongo import DeleteMongo
from database.queries.mongo.selectMongo import SelectMongo
from database.queries.mongo.updateMongo import UpdateMongo
from database.queries.mysql.deleteSQL import DeleteSQL
from database.queries.mysql.selectSQL import SelectSQL
from database.queries.mysql.updateSQL import UpdateSQL
from pymongo import MongoClient
from utils.commons import (CONSOLE_PROMPT, WRONG_COMMAND,
                           NOT_IMPLEMENTED, CLOSE_APP,
                           DB_HOST, DB_USER, DB_PASS, DB_NAME, NUMBER_OF_DATA, BATCH_SIZE)
from utils.logger import Logger


def print_help(command : str)-> None:
    if command == "c":
        print("  [c]   compare databases")
        print("  params:")
        print("    <n> number of iterations that will be performed")
    elif command == "l":
        print("  [l]   show program logs")
    elif command == "q":
        print("  [q]   quit application")
    elif command == "h":
        print("  [h]   display available commands")
        print("  params:")
        print("    <cmd> show command help")
    else:
        print(WRONG_COMMAND)


def main_menu(cmp : bool) -> None:
    print("Database comparison tool v1.0")
    print("Usage: <command> <optional params>")
    print("Available commands: ")
    print("  [c]       compare databases")
    if cmp:
        print("  [g]       create comparison graphs")
    print("  [l]       show program logs")
    print("  [h]       display available commands")
    print("  [q]       quit application")


def progress_bar(msg : str, progress : int) -> None:
    if progress == 50:
        print(f"\r{msg}", end="")
        print("")
    else:
        print(f"\r{msg} [" + "#" * progress + " " * (50 - progress) + "]", end="")


def insert_db(num_elements : int, batch_size : int) -> None:
    Generator.generate_files(num_elements, batch_size)


def update_db() -> None:
    connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
    )
    client = MongoClient("mongodb://admin:password@localhost:27017/")
    database = client[DB_NAME]
    mysql_stats = StatsCollector("mysql", "UPDATE", 0, 0)
    mongo_stats = StatsCollector("mongo", "UPDATE", 0, 0)

    Logger.log("INFO", "Performing UPDATE operations")

    # MYSQL
    msg = "[UPDATE] Password and other fields when user is admin"
    progress_bar(msg, 0)
    result = UpdateSQL.update_password_and_other_fields_when_user_is_admin(connection)
    mysql_stats.add_stats(result["total_time"], result["affected_rows"])
    # MONGO
    progress_bar(msg, 10)
    result, mongo_time = UpdateMongo.update_password_and_other_fields_when_user_is_admin(database)
    mongo_stats.add_stats(mongo_time, result.modified_count)

    # MYSQL
    msg = "[UPDATE] Reservation time"
    progress_bar(msg, 10)
    result = UpdateSQL.update_reservation_time(connection)
    mysql_stats.add_stats(result["total_time"], result["affected_rows"])
    # MONGO
    progress_bar(msg, 20)
    result, mongo_time = UpdateMongo.update_reservation_time(database)
    mongo_stats.add_stats(mongo_time, result.modified_count)

    # MYSQL
    msg = "[UPDATE] Status when problem is older than"
    progress_bar(msg, 20)
    result = UpdateSQL.update_status_when_problem_is_older_than(connection)
    mysql_stats.add_stats(result["total_time"], result["affected_rows"])
    # MONGO
    progress_bar(msg, 30)
    result, mongo_time = UpdateMongo.update_status_when_problem_is_older_than(database)
    mongo_stats.add_stats(mongo_time, result.modified_count)

    # MYSQL
    msg = "[UPDATE] One message"
    progress_bar(msg, 30)
    result = UpdateSQL.update_one_message(connection)
    mysql_stats.add_stats(result["total_time"], result["affected_rows"])
    # MONGO
    progress_bar(msg, 40)
    result, mongo_time = UpdateMongo.update_one_message(database)
    mongo_stats.add_stats(mongo_time, result.modified_count)

    connection.close()
    Logger.save_stats(mysql_stats)
    Logger.save_stats(mongo_stats)
    progress_bar("[UPDATE] Completed", 50)
    Logger.log("INFO", f"MySQL - data updated in {mysql_stats.total_time:.02f} seconds.")
    Logger.log("INFO", f"Mongo - data updated in {mongo_stats.total_time:.02f} seconds.")


def select_db() -> None:
    connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
    )
    client = MongoClient("mongodb://admin:password@localhost:27017/")
    database = client[DB_NAME]
    mysql_stats = StatsCollector("mysql", "SELECT", 0, 0)
    mongo_stats = StatsCollector("mongo", "SELECT", 0, 0)
    Logger.log("INFO", "Performing SELECT operations")

    # MYSQL
    msg = "[SELECT] Group reservation per device"
    progress_bar(msg, 0)
    response, elapsed = SelectSQL.group_reservation_per_device(connection)
    mysql_stats.add_stats(elapsed, len(list(response)))
    # MONGO
    progress_bar(msg, 5)
    response, elapsed = SelectMongo.group_reservation_per_device(database)
    mongo_stats.add_stats(elapsed, len(list(response)))

    # MYSQL
    msg = "[SELECT] Count average reservation time per device"
    progress_bar(msg, 5)
    response, elapsed = SelectSQL.count_average_reservation_time_per_device(connection)
    mysql_stats.add_stats(elapsed, len(list(response)))
    # MONGO
    progress_bar(msg, 10)
    response, elapsed = SelectMongo.count_average_reservation_time_per_device(database)
    mongo_stats.add_stats(elapsed, len(list(response)))

    # MYSQL
    msg = "[SELECT] Count number of admins"
    progress_bar(msg, 10)
    response, elapsed = SelectSQL.count_number_of_admins(connection)
    mysql_stats.add_stats(elapsed, len(list(response)))
    # MONGO
    progress_bar(msg, 15)
    response, elapsed = SelectMongo.count_number_of_admins(database)
    mongo_stats.add_stats(elapsed, len(list(response)))

    # MYSQL
    msg = "[SELECT] Find problem by id"
    progress_bar(msg, 15)
    response, elapsed = SelectSQL.find_problem_by_id(connection, '000f28d2-eb2b-586a-949a-6439378f4d18')
    mysql_stats.add_stats(elapsed, len(list(response)))
    # MONGO
    progress_bar(msg, 20)
    response, elapsed = SelectMongo.find_problem_by_id(database, '674f8f8e8872d55fbc96ed06')
    mongo_stats.add_stats(elapsed, len(list(response)))

    # MYSQL
    msg = "[SELECT] Find dormitory messages"
    progress_bar(msg, 20)
    response, elapsed = SelectSQL.find_dormitory_messages(connection, '00059349-f566-566d-a89d-c423576285d0')
    mysql_stats.add_stats(elapsed, len(list(response)))
    # MONGO
    progress_bar(msg, 25)
    response, elapsed = SelectMongo.find_dormitory_messages(database, '674f8f8b8872d55fbc96d8b9')
    mongo_stats.add_stats(elapsed, len(list(response)))

    # MYSQL
    msg = "[SELECT] Find reservations earlier then concrete data"
    progress_bar(msg, 25)
    response, elapsed = SelectSQL.find_reservations_earlier_then_concrete_data(connection)
    mysql_stats.add_stats(elapsed, len(list(response)))
    # MONGO
    progress_bar(msg, 30)
    response, elapsed = SelectMongo.find_reservations_earlier_then_concrete_data(database)
    mongo_stats.add_stats(elapsed, response)

    # MYSQL
    msg = "[SELECT] Find users"
    progress_bar(msg, 30)
    response, elapsed = SelectSQL.find_users(connection)
    mysql_stats.add_stats(elapsed, len(list(response)))
    # MONGO
    progress_bar(msg, 35)
    response, elapsed = SelectMongo.find_users(database)
    mongo_stats.add_stats(elapsed, len(list(response)))

    # MYSQL
    msg = "[SELECT] Count problems by status"
    progress_bar(msg, 35)
    response, elapsed = SelectSQL.count_problems_by_status(connection)
    mysql_stats.add_stats(elapsed, sum(r['count'] for r in response))
    # MONGO
    progress_bar(msg, 40)
    response, elapsed = SelectMongo.count_problems_by_status(database)
    mongo_stats.add_stats(elapsed, sum(r['count'] for r in list(response)))

    # MYSQL
    msg = "[SELECT] Count problems by status"
    progress_bar(msg, 40)
    response, elapsed = SelectSQL.group_messages_per_dormitory(connection)
    mysql_stats.add_stats(elapsed, len(list(response)))
    # MONGO
    progress_bar(msg, 40)
    response, elapsed = SelectMongo.group_messages_per_dormitory(database)
    mongo_stats.add_stats(elapsed, len(list(response)))

    connection.close()
    progress_bar("[SELECT] Completed", 50)
    Logger.save_stats(mysql_stats)
    Logger.save_stats(mongo_stats)
    Logger.log("INFO", f"MySQL - data selected in {mysql_stats.total_time:.02f} seconds.")
    Logger.log("INFO", f"Mongo - data selected in {mongo_stats.total_time:.02f} seconds.")


def delete_db() -> None:
    connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
    )
    client = MongoClient("mongodb://admin:password@localhost:27017/")
    database = client[DB_NAME]
    mysql_stats = StatsCollector("mysql", "DELETE", 0, 0)
    mongo_stats = StatsCollector("mongo", "DELETE", 0, 0)
    Logger.log("INFO", "Performing DELETE operations")

    # MYSQL
    dorm_names = [f"Dormitory {i}" for i in range(1, 10)]
    msg = "[DELETE] Many not in batch"
    progress_bar(msg, 0)
    elapsed = DeleteSQL.delete_many_not_in_batch(connection, dorm_names)
    mysql_stats.add_stats(elapsed, len(dorm_names))
    # MONGO
    progress_bar(msg, 12)
    elapsed = DeleteMongo.delete_many_not_in_batch(database, dorm_names)
    mongo_stats.add_stats(elapsed, len(dorm_names))

    # MYSQL
    msg = "[DELETE] Many in batch"
    progress_bar(msg, 12)
    dorm_names = [f"Dormitory {i}" for i in range(10, 20)]
    placeholders = ', '.join(['%s'] * len(dorm_names))
    elapsed = DeleteSQL.delete_many_in_batch(connection, dorm_names, placeholders)
    mysql_stats.add_stats(elapsed, len(dorm_names))
    # MONGO
    progress_bar(msg, 24)
    elapsed = DeleteMongo.delete_many_in_batch(database, dorm_names)
    mongo_stats.add_stats(elapsed, len(dorm_names))

    # MYSQL
    dorm_name = f"Dormitory 3979"
    msg = "[DELETE] One dormitory"
    progress_bar(msg, 24)
    elapsed = DeleteSQL.delete_one(connection, dorm_name)
    mongo_stats.add_stats(elapsed, 1)
    # MONGO
    progress_bar(msg, 36)
    elapsed = DeleteMongo.delete_one(database, dorm_name)
    mongo_stats.add_stats(elapsed, 1)

    connection.close()
    progress_bar("[DELETE] Completed", 50)
    Logger.save_stats(mysql_stats)
    Logger.save_stats(mongo_stats)
    Logger.log("INFO", f"MySQL - data deleted in {mysql_stats.total_time:.02f} seconds.")
    Logger.log("INFO", f"Mongo - data deleted in {mongo_stats.total_time:.02f} seconds.")


def compare(iters : int) -> bool:
    Logger.log("INFO", "Comparing databases - start")
    for index, num_elements in enumerate(NUMBER_OF_DATA):
        batch_size = BATCH_SIZE[index]
        Logger.log("INFO", f"Number of data: {num_elements}")
        for i in range(iters):
            Logger.log("INFO", f"Iteration {i + 1} / {iters}")
            insert_db(num_elements, batch_size)
            select_db()
            update_db()
            delete_db()
    Logger.log("INFO", "Comparing databases - finished")
    return True

if __name__ == '__main__':
    compared = False
    print()
    main_menu(compared)
    while True:
        op = input(CONSOLE_PROMPT).lower()
        tmp = op.split(" ")
        if len(tmp) == 2:
            op, opt = tmp[0].strip(), tmp[1].strip()
        else:
            op = tmp[0].strip()
            opt = ""
        if op == "q":
            print(CLOSE_APP)
            break
        if op == "c":
            try:
                opt = int(opt)
            except ValueError:
                Logger.log("INFO", "Using default value (1).")
                opt = 1
            compared = compare(opt)
        elif op == "g":
            if not compared:
                continue
            print(NOT_IMPLEMENTED)
        elif op == "l":
            Logger.print_logs()
        elif op == "h":
            if opt == "":
                main_menu(compared)
            else:
                print_help(opt)
        else:
            print(WRONG_COMMAND)

# TODO: - DELETE - można by przetestować większe zakresy ?
#       - Wykresy
#       - Przenieść operacje z main.py do osobnych plików -> będzie czytelniej
#       - Pasek postępu ustawiany w zależności od długości stringów
#       - Można dodać pasek postępu do INSERTów