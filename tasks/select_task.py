from math import floor

from data.stats_collector import StatsCollector
from database.queries.mongo.selectMongo import SelectMongo
from database.queries.mysql.selectSQL import SelectSQL
from tasks.task import Task
from utils.commons import PROGRESS_BAR_LENGTH, SELECT_MSG
from utils.logger import Logger


class SelectTask(Task):
    def __init__(self):
        super().__init__()
        self.operation = "SELECT"
        self.mysql_stats = StatsCollector("mysql", self.operation, 0, 0)
        self.mongo_stats = StatsCollector("mongo", self.operation, 0, 0)
        self.messages = SELECT_MSG
        self.progress_add = int(floor(PROGRESS_BAR_LENGTH / len(self.messages)))

    def run(self) -> None:
        if not self.connected:
            self.connect()

        self.progress = 0
        self.index = 0
        Logger.log("INFO", "Performing SELECT operations")

        # MYSQL
        self.progress_bar()
        response, elapsed = SelectSQL.group_reservation_per_device(self.connection)
        self.mysql_stats.add_stats(elapsed, len(list(response)))
        # MONGO
        self.progress_bar(1)
        response, elapsed = SelectMongo.group_reservation_per_device(self.database)
        self.mongo_stats.add_stats(elapsed, len(list(response)))

        # MYSQL
        self.progress_bar(2)
        response, elapsed = SelectSQL.count_average_reservation_time_per_device(self.connection)
        self.mysql_stats.add_stats(elapsed, len(list(response)))
        # MONGO
        self.progress_bar(1)
        response, elapsed = SelectMongo.count_average_reservation_time_per_device(self.database)
        self.mongo_stats.add_stats(elapsed, len(list(response)))

        # MYSQL
        self.progress_bar(2)
        response, elapsed = SelectSQL.count_number_of_admins(self.connection)
        self.mysql_stats.add_stats(elapsed, len(list(response)))
        # MONGO
        self.progress_bar(1)
        response, elapsed = SelectMongo.count_number_of_admins(self.database)
        self.mongo_stats.add_stats(elapsed, len(list(response)))

        # MYSQL
        self.progress_bar(2)
        response, elapsed = SelectSQL.find_problem_by_id(self.connection, '000f28d2-eb2b-586a-949a-6439378f4d18')
        self.mysql_stats.add_stats(elapsed, len(list(response)))
        # MONGO
        self.progress_bar(1)
        response, elapsed = SelectMongo.find_problem_by_id(self.database, '674f8f8e8872d55fbc96ed06')
        self.mongo_stats.add_stats(elapsed, len(list(response)))

        # MYSQL
        self.progress_bar(2)
        response, elapsed = SelectSQL.find_dormitory_messages(self.connection, '00059349-f566-566d-a89d-c423576285d0')
        self.mysql_stats.add_stats(elapsed, len(list(response)))
        # MONGO
        self.progress_bar(1)
        response, elapsed = SelectMongo.find_dormitory_messages(self.database, '674f8f8b8872d55fbc96d8b9')
        self.mongo_stats.add_stats(elapsed, len(list(response)))

        # MYSQL
        self.progress_bar(2)
        response, elapsed = SelectSQL.find_reservations_earlier_then_concrete_data(self.connection)
        self.mysql_stats.add_stats(elapsed, len(list(response)))
        # MONGO
        self.progress_bar(1)
        response, elapsed = SelectMongo.find_reservations_earlier_then_concrete_data(self.database)
        self.mongo_stats.add_stats(elapsed, response)

        # MYSQL
        self.progress_bar(2)
        response, elapsed = SelectSQL.find_users(self.connection)
        self.mysql_stats.add_stats(elapsed, len(list(response)))
        # MONGO
        self.progress_bar(1)
        response, elapsed = SelectMongo.find_users(self.database)
        self.mongo_stats.add_stats(elapsed, len(list(response)))

        # MYSQL
        self.progress_bar(2)
        response, elapsed = SelectSQL.count_problems_by_status(self.connection)
        self.mysql_stats.add_stats(elapsed, sum(r['count'] for r in response))
        # MONGO
        self.progress_bar(1)
        response, elapsed = SelectMongo.count_problems_by_status(self.database)
        self.mongo_stats.add_stats(elapsed, sum(r['count'] for r in list(response)))

        # MYSQL
        self.progress_bar(2)
        response, elapsed = SelectSQL.group_messages_per_dormitory(self.connection)
        self.mysql_stats.add_stats(elapsed, len(list(response)))
        # MONGO
        self.progress_bar(1)
        response, elapsed = SelectMongo.group_messages_per_dormitory(self.database)
        self.mongo_stats.add_stats(elapsed, len(list(response)))

        self.connection.close()

        self.progress_bar(3)
        Logger.save_stats(self.mysql_stats)
        Logger.save_stats(self.mongo_stats)
        Logger.log("INFO", f"MySQL - data selected in {self.mysql_stats.total_time:.02f} seconds.")
        Logger.log("INFO", f"Mongo - data selected in {self.mongo_stats.total_time:.02f} seconds.")