from psycopg.rows import dict_row


class Channels:
    def __init__(self, db, hostname):
        self.db_connection = db
        self.cur = db.cursor(row_factory=dict_row)
        self.id, self.type = self._get_channel_id_and_type(hostname)

    def _get_channel_id_and_type(self, channel_name):
        query = """SELECT channel_id, channel_type FROM channels WHERE channel_name = %s"""
        self.cur.execute(query, (channel_name,))
        row = self.cur.fetchone()
        return row['channel_id'], row['channel_type']