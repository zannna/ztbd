import mysql.connector
from data.generation import Generator
from utils.commons import (CONSOLE_PROMPT, MONGO_PROMPT, MYSQL_PROMPT, WRONG_COMMAND,
                           NOT_IMPLEMENTED, CLOSE_APP, ELEMENTS_COUNT, BATCH_SIZE, INVALID_VALUE,
                           BATCH_SIZE_INFO, CREATE_DATABASE, DEFAULT_SIZE, VALUE_TOO_LOW, VALUE_TOO_BIG,
                           DATABASE_NOT_EXIST, DB_HOST, DB_USER, DB_PASS, DB_NAME, PATH_TO_STATS)
from utils.logger import Logger


def main_menu(cmp : bool) -> None:
    print("\nDatabase comparison tool v1.0")
    print("Usage: <command> ")
    print("Available commands: ")
    print("  [create]  create databases and generate data")
    print("  [mongo]   perform operations in MongoDB database")
    print("  [mysql]   perform operations in MySQL database")
    if cmp:
        print("  [g]       create comparison graphs")
    print("  [l]       show program logs")
    print("  [h]       display available commands")
    print("  [q]       quit application")


def database_menu(db: str, prompt : str) -> None:
    print(f"Usage: {prompt}<command>")
    print("Available commands: ")
    print("  [s]   perform SELECT operations")
    print("  [u]   perform UPDATE operations")
    print("  [d]   perform DELETE operations")
    print("  [h]   show help")
    print("  [q]   quit {}".format(db))


def get_params(msg : str) -> int:
    while True:
        x = input(f"{msg}")
        if x.lower() == "q":
            return -1
        if x.lower().strip() == "":
            return DEFAULT_SIZE
        try:
            x = int(x)
            if x < 1000:
                raise Exception(VALUE_TOO_LOW)
            if x > 1000000:
                raise Exception(VALUE_TOO_BIG)
            break
        except ValueError:
            print(f"{INVALID_VALUE}")
        except Exception as e:
            print(f"{INVALID_VALUE} - {str(e)}")
    return x


def create_databases() -> bool:
    print(f"{CREATE_DATABASE}")
    num_elements = get_params(ELEMENTS_COUNT)
    if num_elements == -1:
        return False
    print(f"[INFO] Elements count: {num_elements}")
    batch_size = get_params(BATCH_SIZE)
    if batch_size == -1:
        return False
    print(f"[INFO] Batch size: {batch_size}")
    if batch_size > num_elements:
        print(f"{BATCH_SIZE_INFO}")
        batch_size = DEFAULT_SIZE
    Generator.generate_files(num_elements, batch_size)
    return True


def update_db(db : str) -> None:
    if db == "mysql":
        connection = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASS,
            database=DB_NAME
        )
    elif db == "mongo":
        pass


def select_db(db : str) -> None:
    if db == "mysql":
        pass
    elif db == "mongo":
        pass


def delete_db(db : str) -> None:
    if db == "mysql":
        pass
    elif db == "mongo":
        pass


def database_options(prompt : str, db_created : bool, db : str) -> None:
    assert db == "mysql" or db == "mongo"
    while True:
        operation = input(prompt).lower()
        if operation == "q":
            return

        elif operation == "s":
            if not db_created:
                print(DATABASE_NOT_EXIST(prompt))
                return
            # TODO : SELECT
            print(NOT_IMPLEMENTED(prompt))

        elif operation == "u":
            if not db_created:
                print(DATABASE_NOT_EXIST(prompt))
                return
            update_db(db)

        elif operation == "d":
            if not db_created:
                print(DATABASE_NOT_EXIST(prompt))
                return

            # TODO : DELETE
            print(NOT_IMPLEMENTED(prompt))

        elif operation == "h":
            database_menu(db, prompt)

        else:
            print(WRONG_COMMAND(prompt))


def compare() -> bool:
    return False


if __name__ == '__main__':
    compared = False
    main_menu(compared)
    while True:
        op = input(CONSOLE_PROMPT).lower()
        if op == "q":
            print(CLOSE_APP(CONSOLE_PROMPT))
            break
        if op == "c":
            compared = compare()
        elif op == "create":
            created = create_databases()
        elif op == "mysql":
            database_options(MYSQL_PROMPT, created, "mysql")
        elif op == "mongo":
            database_options(MONGO_PROMPT, created, "mongo")
        elif op == "g":
            if not compared:
                continue
            # TODO
            print(NOT_IMPLEMENTED(CONSOLE_PROMPT))
        elif op == "l":
            Logger.print_logs()
        elif op == "h":
            main_menu(compared)
        else:
            print(WRONG_COMMAND(CONSOLE_PROMPT))