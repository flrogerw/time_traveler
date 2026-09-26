#!/usr/bin/env python3
"""
One-time fix for the 29 same-(network, day_of_week, air_date, air_time) collisions in
`schedules` found while validating the weekend scheduling patch (see
weekend_schedule_collision_research_brief.md / weekend_schedule_collision_resolutions.csv on the
Desktop -- ChatGPT-researched resolutions against real historical network schedule grids).

IMPORTANT: several of the research file's suggested "move to time X" fixes were checked against
the current DB and would have created a *new* collision at the target slot (a different,
already-correct show already occupies it there) -- e.g. ABC 1967 11:00 already has "The New
Beatles", so George of the Jungle can't move there as suggested. Every action below was verified
against the actual current occupant of its target slot before being included; see the
conversation this script came out of for the full row-by-row reasoning.

Two conflicts are deliberately left unresolved -- both are genuine same-calendar-year,
different-part-of-season occupants that `schedules`' year-only date granularity can't split:
  - CBS Saturday 1965 20:30 (Secret Agent vs. The Trials of O'Brien) -- untouched, no action
    taken by this script.
  - ABC Saturday 1966 19:30 -- moving The Adventures of Ozzie and Harriet's row from 1965 to
    1966 (see MOVE_YEAR below; it did genuinely continue into 1966) collided with an
    already-present, independently-correct "Shane" row also at ABC Sat 19:30 1966 (Shane was a
    real fall-1966 replacement series in that same slot). Discovered only after applying the
    move -- see the conversation this script came from. Left as-is; both rows are real for
    different parts of 1966.

Idempotent: every action is keyed by `schedule_id`, and re-running is a no-op once applied
(deletes of already-deleted rows are silently skipped by their WHERE clause; updates of
already-updated rows just re-apply the same value).
"""
import os

import psycopg
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    return psycopg.connect(
        host=os.environ["DB_HOST"],
        port=os.environ.get("DB_PORT", "5432"),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        dbname=os.environ.get("DATABASE") or os.environ["DB_NAME"],
    )


# schedule_id -> reason, purely for the printed log.
DELETE_IDS = {
    4143: "Jonny Quest wrongly tagged ABC Sat 1965 08:30 -- it was an ABC Friday prime-time show",
    4144: "Beatles duplicate at 09:00 -- correct Beatles row already at 08:30 (4225)",
    4136: "Bugs Bunny/Road Runner is a CBS show, wrongly tagged ABC 1967 08:00",
    4312: "New Casper Cartoon Show wrong-time for ABC 1967 08:00; real 08:30 slot already held by The Fantastic Four",
    4314: "Spider-Man wrong-time duplicate at ABC 1967 09:00 -- real Spider-Man:TAS row retimed into 09:30 instead",
    4147: "George of the Jungle: real slot (11:00) already occupied by The New Beatles -- dropping rather than creating a new collision",
    4316: "King Kong not in the ABC 1967 grid at this slot",
    4137: "Bugs Bunny/Road Runner wrongly tagged ABC 1968 08:00 (CBS show)",
    4361: "New Casper Cartoon Show wrong-time for ABC 1968 08:00; real 08:30 slot already held by The Adventures of Gulliver",
    4149: "Spider-Man:TAS not the ABC 1968 10:00 show; its usual 09:30 slot is already Fantastic Voyage -- dropping",
    4138: "Bugs Bunny/Road Runner wrongly tagged ABC 1969 08:00 (CBS show)",
    4408: "New Casper Cartoon Show wrong-time for ABC 1969 08:00; real 08:30 slot already held by The Smokey Bear Show",
    4150: "Spider-Man:TAS not the ABC 1969 10:00 show; its usual 09:30 slot is already Hot Wheels -- dropping",
    4412: "Hardy Boys not the ABC 1969 10:00 show; its 10:30 slot is already Skyhawks -- dropping",
    4102: "The Brady Bunch Hour was a sporadic preemption of the regular Hardy Boys/Nancy Drew Mysteries slot, not a competing recurring template",
    4121: "Bugs Bunny/Road Runner not the CBS 1966 08:00 show (Captain Kangaroo is); no confirmed alternate CBS slot for 1966",
    4122: "Bugs Bunny/Road Runner not the CBS 1967 08:00 show (Captain Kangaroo is); no confirmed alternate CBS slot for 1967",
    4123: "Bugs Bunny/Road Runner Show duplicate of the already-present (now show_id-linked) Bugs Bunny/Road Runner Hour at CBS 1968 08:30",
    4124: "Bugs Bunny/Road Runner Show duplicate of the already-present (now show_id-linked) Bugs Bunny/Road Runner Hour at CBS 1969 08:30",
    4164: "Underdog duplicate at NBC 1965 08:30 -- correct Underdog row already at 09:30 (4259)",
    4167: "Atom Ant duplicate at NBC 1965 09:00 -- correct Atom Ant row is 4257 at 08:30 (now show_id-linked)",
    4170: "Secret Squirrel duplicate at NBC 1965 09:30 -- correct Secret Squirrel row is 4258 at 09:00 (now show_id-linked)",
    4165: "Underdog not the NBC 1966 08:30 show (moved slot/network that season)",
    4168: "Atom Ant not the NBC 1966 09:00 show",
    4301: "The Flintstones was only a winter reshuffle addition at NBC 1966 09:00, not the fall/year default -- schedules has no sub-year granularity to represent that",
    4166: "Underdog not the NBC 1967 08:30 show (Super President and Spy Shadow is)",
    4169: "Atom Ant not the NBC 1967 09:00 show (The Flintstones is)",
    4172: "Pink Panther duplicate at NBC 1969 10:00 -- correct Pink Panther row already at 09:00 (4444)",
}

# schedule_id -> new air_time (string, 'HH:MM:SS'). Every target slot was checked and confirmed
# empty (or emptied by this same script's deletes) before being included.
RETIME = {
    4270: "08:30:00",  # King Kong, ABC 1966: was wrongly at 09:00 (Beatles' slot)
    4315: "10:00:00",  # Journey to the Center of the Earth, ABC 1967: was wrongly at 09:30
    4148: "09:30:00",  # Spider-Man: The Animated Series, ABC 1967: was wrongly at 10:00
    4171: "09:00:00",  # Secret Squirrel, NBC 1966: was wrongly at 09:30
}

# schedule_id -> (air_date column) old year -> new year. Genuine mid-season replacements that
# got recorded against the wrong calendar year.
MOVE_YEAR = {
    4086: 1966,  # The Adventures of Ozzie and Harriet, ABC Sat 19:30 -- began 1966-01-15
    4088: 1966,  # The Donna Reed Show, ABC Sat 20:00 -- began 1966-01-15
    4096: 1966,  # The Face is Familiar, CBS Sat 21:30 -- began 1966-05-07
}

# schedule_id -> show_id. Rows that were already correctly placed but had no show_id link yet
# (same real program already ingested under a different title elsewhere in the table).
LINK_SHOW_ID = {
    4257: 36,   # "The Atom Ant Show" == show_id 36 (Atom Ant), NBC 1965 08:30
    4258: 43,   # "The Secret Squirrel Show" == show_id 43 (Secret Squirrel), NBC 1965 09:00
    4377: 343,  # "The Bugs Bunny/Road Runner Hour" == show_id 343, CBS 1968 08:30
    4426: 343,  # "The Bugs Bunny/Road Runner Hour" == show_id 343, CBS 1969 08:30
}

FLAGGED_UNRESOLVED = [4093, 4094]  # Secret Agent / The Trials of O'Brien, CBS Sat 1965 20:30


def main():
    conn = get_db_connection()
    cur = conn.cursor()

    for schedule_id, new_show_id in LINK_SHOW_ID.items():
        cur.execute("UPDATE schedules SET show_id = %s WHERE schedule_id = %s", (new_show_id, schedule_id))
    print(f"Linked show_id on {len(LINK_SHOW_ID)} rows.")

    for schedule_id, new_time in RETIME.items():
        cur.execute("UPDATE schedules SET air_time = %s WHERE schedule_id = %s", (new_time, schedule_id))
    print(f"Retimed {len(RETIME)} rows.")

    for schedule_id, new_year in MOVE_YEAR.items():
        cur.execute("UPDATE schedules SET air_date = %s WHERE schedule_id = %s", (new_year, schedule_id))
    print(f"Moved {len(MOVE_YEAR)} rows to their correct calendar year.")

    for schedule_id, reason in DELETE_IDS.items():
        cur.execute("DELETE FROM schedules WHERE schedule_id = %s", (schedule_id,))
    print(f"Deleted {len(DELETE_IDS)} wrong/duplicate rows.")

    conn.commit()

    # Verify: re-run the same collision query used to find these in the first place.
    cur.execute(
        """
        SELECT network, day_of_week, air_date, air_time, count(distinct show_title) AS n
        FROM schedules
        WHERE day_of_week IN ('saturday', 'sunday')
        GROUP BY 1, 2, 3, 4
        HAVING count(distinct show_title) > 1
        ORDER BY 1, 3, 2, 4
        """
    )
    remaining = cur.fetchall()
    print(f"\nRemaining weekend collisions: {len(remaining)} (expected: 2, both flagged/unresolved -- see module docstring)")
    for row in remaining:
        print(" ", row)

    conn.close()


if __name__ == "__main__":
    main()
