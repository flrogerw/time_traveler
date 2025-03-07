import psycopg2
import random

# Database connection
con = psycopg2.connect(database="time_traveler", user="postgres", password="m06Ar14u", host="192.168.1.201", port=5432)
cur = con.cursor()

# Target missing time in seconds (4 minutes 52 seconds = 292 seconds)
target_duration = 308
year = 1965

# Query commercials with durations less than or equal to target time
cur.execute("""
    SELECT commercial_id, commercial_end - commercial_start as duration FROM commercials
    WHERE commercial_end - commercial_start <= %s 
    AND commercial_airdate >= %s AND commercial_airdate <= %s
    ORDER BY random();
""", (target_duration, year - 3, year))

commercials = cur.fetchall()


# Randomized search for the best combination of commercials
def randomized_search(commercials, target_duration, max_attempts=100):
    best_combination = []
    best_remaining_time = target_duration  # Start with the full target duration as the "best"
    print(commercials)
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


# Run randomized search 100 times to find the best solution
best_commercials = randomized_search(commercials, target_duration, max_attempts=100)

print("Best commercials to fill missing time:", best_commercials)

# Close the database connection
cur.close()
con.close()
