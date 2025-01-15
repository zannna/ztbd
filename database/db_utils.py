from utils.commons import DB_NAME
import uuid

class DbUtils:
    @staticmethod
    def clear_database_mongo(client):
        client.drop_database(DB_NAME)

    @staticmethod
    def clear_database(mysql_conn):
        cursor = mysql_conn.cursor()
        cursor.execute(f"DROP DATABASE IF EXISTS {DB_NAME}")
        cursor.execute(f"CREATE DATABASE {DB_NAME}")
        mysql_conn.commit()

    @staticmethod
    def create_mysql_database(mysql_conn):
        cursor = mysql_conn.cursor()
        for statement in CREATE_TABLES_SQL.split(";"):
            if statement.strip():
                cursor.execute(statement)
        mysql_conn.commit()

    @staticmethod
    def generate_uuid():
        return str(uuid.uuid4())

    @staticmethod
    def convert_mongo_id_to_uuid(id):
        return str(uuid.uuid5(NAMESPACE, str(id)))


# CONST
CREATE_TABLES_SQL = """
USE dormitory_management_system;
-- Tabela dormitory (akademiki)
CREATE TABLE IF NOT EXISTS dormitory (
    id_dorm CHAR(36) PRIMARY KEY,
    dorm_name VARCHAR(255) NOT NULL
);

-- Tabela app_user (użytkownicy)
CREATE TABLE IF NOT EXISTS app_user (
    id_user  CHAR(36) PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    first_name VARCHAR(255) NOT NULL,
    surname VARCHAR(255) NOT NULL,
    password VARCHAR(255) NOT NULL,
    room INTEGER,
    university VARCHAR(255),
    id_dorm CHAR(36) REFERENCES dormitory(id_dorm) ON DELETE SET NULL
);

-- Tabela device (urządzenia)
CREATE TABLE IF NOT EXISTS device (
    id_device CHAR(36) PRIMARY KEY,
    name_device VARCHAR(255) NOT NULL,
    number INTEGER,
    work BOOLEAN NOT NULL,
    id_dorm CHAR(36) REFERENCES dormitory(id_dorm) ON DELETE CASCADE
);

-- Tabela problems (problemy użytkowników)
CREATE TABLE IF NOT EXISTS problems (
    id_problem  CHAR(36) PRIMARY KEY,
    description VARCHAR(255) NOT NULL,
    problem_date TIMESTAMP NOT NULL,
    status INTEGER NOT NULL,
    id_user CHAR(36) REFERENCES app_user(id_user) ON DELETE CASCADE,
    id_dorm CHAR(36) REFERENCES dormitory(id_dorm) ON DELETE CASCADE
);

-- Tabela message (wiadomości użytkowników)
CREATE TABLE IF NOT EXISTS message (
    id_mess CHAR(36) PRIMARY KEY,
    mess_date TIMESTAMP NOT NULL,
    message VARCHAR(255) NOT NULL,
    id_user CHAR(36) REFERENCES app_user(id_user) ON DELETE CASCADE
);

-- Tabela reservations (rezerwacje urządzeń)
CREATE TABLE IF NOT EXISTS reservations (
    id_res CHAR(36) PRIMARY KEY,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    id_user CHAR(36) REFERENCES app_user(id_user) ON DELETE CASCADE,
    id_device CHAR(36) REFERENCES device(id_device) ON DELETE CASCADE
);

-- Tabela authority (role użytkowników)
CREATE TABLE IF NOT EXISTS authority (
    id_auth  CHAR(36) PRIMARY KEY UNIQUE,
    authority VARCHAR(255) NOT NULL
);

-- Tabela users_authorities (przypisanie ról do użytkowników)
CREATE TABLE IF NOT EXISTS users_authorities (
    id_user CHAR(36) REFERENCES app_user(id_user) ON DELETE CASCADE,
    id_auth CHAR(36) REFERENCES authority(id_auth) ON DELETE CASCADE,
    PRIMARY KEY (id_user, id_auth)
);
"""

NAMESPACE = uuid.UUID('12345678-1234-5678-1234-567812345678')