#!/usr/bin/env python3
from pprint import pprint

from psycopg2.extras import execute_values, DictCursor
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import logging
import sys


# Sample list of shows
def get_db_connection() -> psycopg2.extensions.connection:
    """Establish and return a database connection."""
    try:
        conn = psycopg2.connect(
            database="time_traveler",
            user="postgres",
            password="m06Ar14u",
            host="192.168.1.201",
            port=5432
        )
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to the database: {e}")
        raise


def get_current_schedule(dow):
    print(dow)
    with get_db_connection() as db:
        cur = db.cursor(cursor_factory=DictCursor)
        cur.execute("""SELECT DISTINCT ON (st.channel_id, st.show_id, st.time_slot)
                            split_part(ch.channel_name, '-', 3) AS channel,
                            st.time_slot,
                            sh.show_duration,
                            sh.show_name AS show,
                            e.episode_title AS title,
                            e.episode_description AS description,
                            bl.date_played
                        FROM schedule_template st
                        LEFT JOIN broadcast_log bl 
                        ON st.channel_id = bl.channel_id AND st.show_id = bl.show_id
                        LEFT JOIN episodes e ON bl.episode_id = e.episode_id
                        LEFT JOIN shows sh ON st.show_id = sh.show_id
                        LEFT JOIN channels ch ON ch.channel_id = bl.channel_id
                        WHERE %s = ANY(st.days_of_week)
                        ORDER BY st.channel_id,st.time_slot, st.show_id, bl.date_played DESC;""", (dow,))
    records = cur.fetchall()
    print(records)
    formatted_records = [{
        **dict(record),
        'time_slot': record['time_slot'].strftime("%H:%M")
    } for record in records]
    return formatted_records


today = datetime.now()
shows = get_current_schedule(today.weekday())


def generate_tv_guide_html(shows, year):
    # Sort shows by channel and time slot
    pprint(shows)
    shows.sort(key=lambda x: (x['channel'], x['time_slot']))
    today = datetime.now()
    past = datetime(int(year), today.month, today.weekday()+1)
    # Header of the HTML
    html = '''
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>TV Guide</title>
        <link rel="stylesheet" href="styles.css"> <!-- Link to external CSS file -->
    </head>
    <body>
        <div class="tv-guide">
            <div class="header">TV Guide - {}</div>
    '''.format(past.strftime("%A, %Y"))

    # Generate time slots from 6:00 PM to 1:00 AM
    start_time = datetime.strptime("18:00", "%H:%M")
    end_time = datetime.strptime("01:00", "%H:%M")
    end_time = end_time.replace(
        day=start_time.day + 1)  # Adjust end_time to the next day to ensure correct calculations
    time_slots = [start_time + timedelta(minutes=30 * i) for i in range(14)]  # 6:00 PM to 1:00 AM

    # Time slot headers
    html += '<div class="channel">Channel</div>'
    for time_slot in time_slots:
        time_label = time_slot.strftime("%I:%M %p")
        html += f'<div class="time">{time_label}</div>'

    # Get all unique channels
    channels = sorted(set(int(show['channel']) for show in shows))

    # Add each channel's schedule
    for channel in channels:
        html += f'<div class="channel">{channel}</div>'
        current_slot = 0

        # Filter shows for the current channel
        channel_shows = [show for show in shows if int(show['channel']) == channel]

        for show in channel_shows:
            show_start = datetime.strptime(show['time_slot'], "%H:%M")
            # Adjust day for show start time if it is after midnight
            if show_start.time() < start_time.time():
                show_start = show_start.replace(day=start_time.day + 1)

            # Calculate the slot index and column span
            slot_index = (show_start - start_time).seconds // 1800
            col_span = show['show_duration'] // 1800

            # Fill empty slots before the show starts
            if slot_index > current_slot:
                html += '<div class="show"></div>' * (slot_index - current_slot)

            # Insert the show with correct spanning
            html += f'<div class="show" style="grid-column: span {col_span};" onclick="showDetails(\'{show["title"]}\', \'{show["description"]}\')">{show["show"]}</div>'
            current_slot = slot_index + col_span

        # Check if there are remaining slots to fill after the last show
        remaining_slots = len(time_slots) - current_slot
        if remaining_slots > 0:
            html += '<div class="show"></div>' * remaining_slots

    # Closing HTML tags
    html += '''
        </div>
        <div id="modal" class="modal">
            <div class="modal-content">
                <span class="close" onclick="closeModal()">&times;</span>
                <h2 id="modal-title"></h2>
                <p id="modal-description"></p>
            </div>
        </div>
    <script>
        function showDetails(title, description) {
        document.getElementById('modal-title').textContent = title;
        document.getElementById('modal-description').textContent = description;
        document.getElementById('modal').style.display = 'block';
        }

        function closeModal() {
            document.getElementById('modal').style.display = 'none';
        }
    </script>
    </body>
    </html>
    '''
    return html


# Generate and save the HTML file
year = sys.argv[1]
html_content = generate_tv_guide_html(shows, year)
with open('tv_guide.html', 'w') as file:
    file.write(html_content)

print(f"HTML file 'tv_guide.html' generated successfully for {year}.")
