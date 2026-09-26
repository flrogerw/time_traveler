# Class to handle cartoon-related database operations
from psycopg.rows import dict_row


class Cartoons:
    def __init__(self, db):
        self.db_connection = db
        self.cur = db.cursor(row_factory=dict_row)