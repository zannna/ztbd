create_tables_sql = """
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

def create_postgres_database(mysql_conn):
    cursor = mysql_conn.cursor()
    for statement in create_tables_sql.split(";"):
        if statement.strip():
            cursor.execute(statement)
    mysql_conn.commit()