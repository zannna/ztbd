from utils.commons import PATH_TO_STATS


class StatsCollector:
    def __init__(self, database : str, operation : str, elements_count : int, batch_size : int) -> None:
        self.database = database
        self.operation = operation
        self.filepath = PATH_TO_STATS + database + '.csv'
        self.elements_count = elements_count
        self.batch_size = batch_size
        self.total_time  = 0.0
        self.records_count = 0
        self.tables_count = 0
        self.batches_count = 0

    def add_stats(self, time : float, records : int, tables : int = 1, batches_count : int = 1) -> None:
        self.total_time += time
        self.records_count += records
        self.tables_count += tables
        self.batches_count += batches_count

    def print_stats(self):
        time_per_record = self.total_time / self.records_count
        time_per_batch = self.total_time / self.batches_count
        time_per_table = self.total_time / self.tables_count

        return (f"{self.operation},"
                f"{self.elements_count},"
                f"{self.batch_size},"
                f"{self.records_count},"
                f"{self.total_time:.08f},"
                f"{time_per_record:.08f},"
                f"{time_per_batch:.08f},"
                f"{time_per_table:.08f}\n")