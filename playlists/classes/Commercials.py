#!/usr/bin/env python3
from psycopg.rows import dict_row
import random


class Commercials:
    def __init__(self, db):
        self.db_connection = db
        self.cur = db.cursor(row_factory=dict_row)

    # Method to retrieve commercial breaks for a given media ID
    def get_commercial_breaks(self, media_id):
        self.cur.execute(f"select * from commercial_breaks where media_id = {media_id} order by break_point;")
        return [dict(record) for record in self.cur.fetchall()]

    @staticmethod
    def randomized_search(commercials, target_duration, max_attempts=100):
        best_combination = []
        best_remaining_time = target_duration  # Start with the full target duration as the "best"

        for _ in range(max_attempts):
            current_combination = []
            remaining_time = target_duration

            # Shuffle commercials to try different combinations each run
            random.shuffle(commercials)

            for commercial_id, duration in commercials:
                if duration <= remaining_time:
                    current_combination.append(commercial_id)
                    remaining_time -= duration

                # If we've exactly filled the time, we can stop early
                if remaining_time == 0:
                    return current_combination

            # If this combination leaves less remaining time, update the best combination
            if remaining_time < best_remaining_time:
                best_combination = current_combination
                best_remaining_time = remaining_time

        return best_combination

    def get_commercials(self, target_duration, year):
        # Case 1: Year = 5000 → ignore year filtering entirely
        if year == 5000:
            query = """
                SELECT *, commercial_id,
                       commercial_end - commercial_start AS duration
                FROM commercials
                WHERE commercial_end - commercial_start <= %s
                ORDER BY random();
            """
            params = (target_duration,)

        # Case 2: Normal behavior → apply airdate range
        else:
            query = """
                SELECT *, commercial_id,
                       commercial_end - commercial_start AS duration
                FROM commercials
                WHERE commercial_end - commercial_start <= %s
                  AND commercial_airdate >= %s
                  AND commercial_airdate <= %s
                ORDER BY random();
            """
            params = (target_duration, year - 3, year)

        # Execute and return results
        self.cur.execute(query, params)
        return [dict(record) for record in self.cur.fetchall()]


