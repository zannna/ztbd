from utils.commons import db_name

def clear_databaseMongo(client):
    client.drop_database(db_name)

def clear_database(mysql_conn):
    cursor = mysql_conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS dormitory_management_system")
    cursor.execute("CREATE DATABASE dormitory_management_system")
    mysql_conn.commit()