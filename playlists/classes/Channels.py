from psycopg2.extras import DictCursor


class Channels:
    def __init__(self, db, hostname):
        self.db_connection = db
        self.cur = db.cursor(cursor_factory=DictCursor)
        self.id = self._get_channel_id(hostname)

    def _get_channel_id(self, channel_name):
        query = """SELECT channel_id FROM channels WHERE channel_name = %s"""
        self.cur.execute(query, (channel_name,))
        return self.cur.fetchone()[0]