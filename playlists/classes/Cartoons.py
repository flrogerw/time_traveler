# Class to handle cartoon-related database operations
from psycopg2.extras import DictCursor


class Cartoons:
    def __init__(self, db):
        self.db_connection = db
        self.cur = db.cursor(cursor_factory=DictCursor)