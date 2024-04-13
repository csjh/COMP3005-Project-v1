import psycopg

try:
    conn = psycopg.connect(
        "dbname='project' user='csjh' host='localhost' password='Execute Order 66'"
    )
except psycopg.OperationalError as e:
    print(f"Error: {e}")
    exit(1)


REFERENCED_TABLES = ["player", "team"]
SPECIAL_TYPES = ["uuid", "time (3)", "smallint", "integer"]

TABLES = {
    "fifty_fifties": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "under_pressure": "boolean",
        "_50_50": {"outcome": "outcome_50_50_enum"},
        "counterpress": "boolean",
        "out": "boolean",
    },
    "bad_behaviours": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "duration": "number",
        "bad_behaviour": {"card": "card_enum"},
        "off_camera": "boolean",
    },
    "ball_receipts": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "ball_receipt": {
            "_nullable": True,
            "outcome": "outcome_ball_receipt_enum | null",
        },
        "under_pressure": "boolean",
    },
    "ball_recoveries": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "under_pressure": "boolean",
        "out": "boolean",
        "ball_recovery": {
            "_nullable": True,
            "recovery_failure": "boolean",
            "offensive": "boolean",
        },
        "off_camera": "boolean",
    },
    "blocks": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "out": "boolean",
        "counterpress": "boolean",
        "block": {
            "_nullable": True,
            "deflection": "boolean",
            "offensive": "boolean",
            "save_block": "boolean",
        },
        "under_pressure": "boolean",
        "off_camera": "boolean",
    },
    "carries": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "carry": {"end_location": "number[]"},
        "under_pressure": "boolean",
    },
    "clearances": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "under_pressure": "boolean",
        "clearance": {
            "right_foot": "boolean",
            "body_part": "bodypart_enum",
            "left_foot": "boolean",
            "aerial_won": "boolean",
            "head": "boolean",
            "other": "boolean",
        },
        "out": "boolean",
        "off_camera": "boolean",
    },
    "dispossessions": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "under_pressure": "boolean",
        "off_camera": "boolean",
    },
    "dribbles": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "under_pressure": "boolean",
        "dribble": {
            "outcome": "outcome_dribble_enum",
            "overrun": "boolean",
            "nutmeg": "boolean",
            "no_touch": "boolean",
        },
        "off_camera": "boolean",
        "out": "boolean",
    },
    "dribbled_pasts": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "counterpress": "boolean",
        "off_camera": "boolean",
    },
    "duels": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "under_pressure": "boolean",
        "duel": {"type": "dueltype_enum", "outcome": "outcome_duel_enum | null"},
        "counterpress": "boolean",
        "off_camera": "boolean",
    },
    "errors": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "under_pressure": "boolean",
        "off_camera": "boolean",
    },
    "fouls_committed": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "counterpress": "boolean",
        "foul_committed": {
            "_nullable": True,
            "type": "foul_enum | null",
            "penalty": "boolean",
            "advantage": "boolean",
            "card": "card_enum | null",
            "offensive": "boolean",
        },
        "off_camera": "boolean",
        "under_pressure": "boolean",
    },
    "fouls_won": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "under_pressure": "boolean",
        "foul_won": {
            "_nullable": True,
            "penalty": "boolean",
            "defensive": "boolean",
            "advantage": "boolean",
        },
        "off_camera": "boolean",
    },
    "goalkeepers": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[] | null",
        "duration": "number",
        "goalkeeper": {
            "outcome": "outcome_goalkeeper_enum | null",
            "technique": "goalkeeper_technique_enum | null",
            "position": "goalkeeper_position_enum | null",
            "body_part": "bodypart_enum | null",
            "type": "goalkeeper_type_enum",
            "end_location": "number[] | null",
            "shot_saved_to_post": "boolean",
            "punched_out": "boolean",
            "success_in_play": "boolean",
            "shot_saved_off_target": "boolean",
            "lost_out": "boolean",
            "lost_in_play": "boolean",
        },
        "out": "boolean",
        "under_pressure": "boolean",
        "off_camera": "boolean",
    },
    "half_ends": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "duration": "number",
        "under_pressure": "boolean",
    },
    "half_starts": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "duration": "number",
        "half_start": {
            "_nullable": True,
            "late_video_start": "boolean",
        },
    },
    "injury_stoppages": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "duration": "number",
        "injury_stoppage": {"_nullable": True, "in_chain": "boolean"},
        "off_camera": "boolean",
        "under_pressure": "boolean",
        "location": "number[] | null",
    },
    "interceptions": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "interception": {"outcome": "outcome_interception_enum"},
        "counterpress": "boolean",
        "under_pressure": "boolean",
        "off_camera": "boolean",
    },
    "miscontrols": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "under_pressure": "boolean",
        "out": "boolean",
        "miscontrol": {
            "_nullable": True,
            "aerial_won": "boolean",
        },
        "off_camera": "boolean",
    },
    "offsides": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
    },
    "own_goals_against": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
    },
    "own_goals_for": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "location": "number[]",
        "duration": "number",
        "player": "player | null",  # might be weird
        "position": "positions_enum | null",
    },
    "passes": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "pass": {
            "recipient": "player | null",
            "length": "number",
            "angle": "number",
            "height": "pass_height_enum",
            "end_location": "number[]",
            "body_part": "bodypart_enum | null",
            "type": "pass_type_enum | null",
            "outcome": "pass_outcome_enum | null",
            "aerial_won": "boolean",
            "assisted_shot_id": "uuid | null",
            "shot_assist": "boolean",
            "switch": "boolean",
            "cross": "boolean",
            "deflected": "boolean",
            "inswinging": "boolean",
            "technique": "pass_technique_enum | null",
            "through_ball": "boolean",
            "no_touch": "boolean",
            "outswinging": "boolean",
            "miscommunication": "boolean",
            "cut_back": "boolean",
            "goal_assist": "boolean",
            "straight": "boolean",
        },
        "under_pressure": "boolean",
        "off_camera": "boolean",
        "counterpress": "boolean",
        "out": "boolean",
    },
    "player_offs": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "duration": "number",
        "off_camera": "boolean",
    },
    "player_ons": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "duration": "number",
        "off_camera": "boolean",
    },
    "pressures": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "counterpress": "boolean",
        "under_pressure": "boolean",
        "off_camera": "boolean",
    },
    "referee_ball_drops": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "location": "number[]",
        "duration": "number",
        "off_camera": "boolean",
    },
    "shields": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "under_pressure": "boolean",
    },
    "shots": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "location": "number[]",
        "duration": "number",
        "shot": {
            "statsbomb_xg": "number",
            "end_location": "number[]",
            "key_pass_id": "uuid | null",
            "body_part": "bodypart_enum",
            "type": "shot_type_enum",
            "outcome": "outcome_shot_enum",
            "first_time": "boolean",
            "technique": "shot_technique_enum",
            # "freeze_frame": "FreezeFrame[] | null",
            "deflected": "boolean",
            "one_on_one": "boolean",
            "aerial_won": "boolean",
            "saved_to_post": "boolean",
            "redirect": "boolean",
            "open_goal": "boolean",
            "follows_dribble": "boolean",
            "saved_off_target": "boolean",
        },
        "under_pressure": "boolean",
        "out": "boolean",
        "off_camera": "boolean",
    },
    "starting_xis": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "duration": "number",
        "tactics": {
            "formation": "integer",
            # "lineup": Lineup[];
        },
    },
    "substitutions": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "player": "player",
        "position": "positions_enum",
        "duration": "number",
        "substitution": {
            "outcome": "outcome_substitution_enum",
            "replacement": "player",
        },
        "off_camera": "boolean",
    },
    "tactical_shifts": {
        "id": "uuid",
        "index": "smallint",
        "period": "smallint",
        "timestamp": "time (3)",
        "minute": "smallint",
        "second": "smallint",
        "type": "event_type_enum",
        "possession": "smallint",
        "possession_team": "team",
        "play_pattern": "play_pattern_enum",
        "team": "team",
        "duration": "number",
        "tactics": {
            "formation": "integer",
            # "lineup": Lineup[];
        },
    },
}


def dict_to_table(table_dict, prefix="", nullable=False):
    query = "\n"

    for key, value in table_dict.items():
        if key == "_nullable":
            continue
        if isinstance(value, dict):
            query += dict_to_table(value, f"{key}_", "_nullable" in value)
        else:
            post = "" if (nullable or value.endswith(" | null")) else "NOT NULL"
            value = value.replace(" | null", "")

            if value == "string":
                query += f"{prefix}{key} TEXT {post},\n"
            elif value == "number":
                query += f"{prefix}{key} DOUBLE PRECISION {post},\n"
            elif value == "boolean":
                query += f"{prefix}{key} BOOLEAN {post},\n"
            elif value == "number[]":
                query += f"{prefix}{key} POINT {post},\n"
            elif value.endswith("enum") or value in SPECIAL_TYPES:
                query += f"{prefix}{key} {value} {post},\n"
            elif value in REFERENCED_TABLES:
                query += f"{prefix}{key}_id INTEGER REFERENCES {value}s({value}_id) {post},\n"
            else:
                print(f"Unknown type: {value}")

    return query


def dict_to_parser(td):
    def parse(given_obj, table_dict=td, prefix=""):
        output = {}
        for key, value in table_dict.items():
            if key == "_nullable":
                continue
            if isinstance(value, dict):
                output = {
                    **output,
                    **parse(
                        given_obj.get(key[1:] if key[0] == "_" else key, {}),
                        value,
                        f"{key}_",
                    ),
                }
                continue
            if value.endswith(" | null") and key not in given_obj:
                if value.replace(" | null", "") in REFERENCED_TABLES:
                    output[f"{prefix}{key}_id"] = None
                else:
                    output[f"{prefix}{key}"] = None
                continue
            value = value.replace(" | null", "")
            if value == "string" or value == "number" or value in SPECIAL_TYPES:
                output[f"{prefix}{key}"] = given_obj[key]
            elif value.endswith("enum"):
                output[f"{prefix}{key}"] = given_obj[key]["name"]
            elif value == "boolean":
                output[f"{prefix}{key}"] = given_obj.get(key, False)
            elif value == "number[]":
                arr = given_obj[key]
                if len(given_obj[key]) == 3:
                    arr[0], arr[1] = arr[1], arr[2]
                output[f"{prefix}{key}"] = f"({arr[0]}, {arr[1]})"
            elif value in REFERENCED_TABLES:
                output[f"{prefix}{key}_id"] = given_obj[key]["id"]
            else:
                print(f"Unknown type: {value}")
        return output

    return parse


def enums():
    with conn.cursor() as cursor:
        cursor.execute("DROP TYPE IF EXISTS event_type_enum CASCADE")
        cursor.execute(
            """
            CREATE TYPE event_type_enum AS ENUM (
                'Foul Won',
                'Pressure',
                'Block',
                'Half End',
                'Error',
                'Shot',
                'Bad Behaviour',
                '50/50',
                'Tactical Shift',
                'Own Goal Against',
                'Shield',
                'Offside',
                'Duel',
                'Referee Ball-Drop',
                'Interception',
                'Half Start',
                'Carry',
                'Pass',
                'Foul Committed',
                'Ball Recovery',
                'Substitution',
                'Miscontrol',
                'Goal Keeper',
                'Player On',
                'Clearance',
                'Starting XI',
                'Ball Receipt*',
                'Injury Stoppage',
                'Dribbled Past',
                'Player Off',
                'Dispossessed',
                'Own Goal For',
                'Dribble'
            )
            """
        )

        cursor.execute("DROP TYPE IF EXISTS play_pattern_enum CASCADE")
        cursor.execute(
            """
            CREATE TYPE play_pattern_enum AS ENUM (
                'Regular Play',
                'From Corner',
                'From Free Kick',
                'From Throw In',
                'Other',
                'From Counter',
                'From Goal Kick',
                'From Keeper',
                'From Kick Off'
            )
            """
        )

        cursor.execute("DROP TYPE IF EXISTS positions_enum CASCADE")
        cursor.execute(
            """
            CREATE TYPE positions_enum AS ENUM (
                'Goalkeeper',
                'Right Back',
                'Right Center Back',
                'Center Back',
                'Left Center Back',
                'Left Back',
                'Right Wing Back',
                'Left Wing Back',
                'Right Defensive Midfield',
                'Center Defensive Midfield',
                'Left Defensive Midfield',
                'Right Midfield',
                'Right Center Midfield',
                'Center Midfield',
                'Left Center Midfield',
                'Left Midfield',
                'Right Wing',
                'Left Attacking Midfield',
                'Right Attacking Midfield',
                'Center Attacking Midfield',
                'Left Attacking',
                'Midfield',
                'Left Wing',
                'Center Forward',
                'Right Center Forward',
                'Striker',
                'Left Center Forward',
                'Secondary Striker',
                'Substitute'
            )
        """
        )

        cursor.execute("DROP TYPE IF EXISTS outcome_50_50_enum CASCADE")
        cursor.execute(
            "CREATE TYPE outcome_50_50_enum AS ENUM ('Won', 'Lost', 'Success To Team', 'Success To Opposition')"
        )

        cursor.execute("DROP TYPE IF EXISTS card_enum CASCADE")
        cursor.execute(
            "CREATE TYPE card_enum AS ENUM ('Yellow Card', 'Second Yellow', 'Red Card')"
        )

        cursor.execute("DROP TYPE IF EXISTS outcome_ball_receipt_enum CASCADE")
        cursor.execute("CREATE TYPE outcome_ball_receipt_enum AS ENUM ('Incomplete')")

        cursor.execute("DROP TYPE IF EXISTS bodypart_enum CASCADE")
        cursor.execute(
            """CREATE TYPE bodypart_enum AS ENUM (
                'Head',
                'Left Foot',
                'Other',
                'Right Foot',
                'Both Hands',
                'Chest',
                'Left Hand',
                'Right Hand',
                'Drop Kick',
                'Keeper Arm',
                'No Touch'
            )"""
        )

        cursor.execute("DROP TYPE IF EXISTS outcome_dribble_enum CASCADE")
        cursor.execute(
            "CREATE TYPE outcome_dribble_enum AS ENUM ('Complete', 'Incomplete')"
        )

        cursor.execute("DROP TYPE IF EXISTS dueltype_enum CASCADE")
        cursor.execute("CREATE TYPE dueltype_enum AS ENUM ('Tackle', 'Aerial Lost')")

        cursor.execute("DROP TYPE IF EXISTS outcome_duel_enum CASCADE")
        cursor.execute(
            "CREATE TYPE outcome_duel_enum AS ENUM ('Lost', 'Won', 'Lost In Play', 'Lost Out', 'Success', 'Success In Play', 'Success Out')"
        )

        cursor.execute("DROP TYPE IF EXISTS foul_enum CASCADE")
        cursor.execute(
            "CREATE TYPE foul_enum AS ENUM ('Foul Out', 'Handball', 'Dangerous Play', '6 Seconds', 'Dive', 'Backpass Pick')"
        )

        cursor.execute("DROP TYPE IF EXISTS goalkeeper_position_enum CASCADE")
        cursor.execute(
            "CREATE TYPE goalkeeper_position_enum AS ENUM ('Set', 'Moving', 'Prone')"
        )

        cursor.execute("DROP TYPE IF EXISTS goalkeeper_technique_enum CASCADE")
        cursor.execute(
            "CREATE TYPE goalkeeper_technique_enum AS ENUM ('Standing', 'Diving')"
        )

        cursor.execute("DROP TYPE IF EXISTS goalkeeper_type_enum CASCADE")
        cursor.execute(
            """
            CREATE TYPE goalkeeper_type_enum AS ENUM (
                'Shot Faced',
                'Shot Saved',
                'Punch',
                'Penalty Conceded',
                'Collected',
                'Shot Saved to Post',
                'Keeper Sweeper',
                'Goal Conceded',
                'Save',
                'Smother',
                'Shot Saved Off Target',
                'Penalty Saved',
                'Penalty Saved to Post'
            )
            """
        )

        cursor.execute("DROP TYPE IF EXISTS outcome_goalkeeper_enum CASCADE")
        cursor.execute(
            """
            CREATE TYPE outcome_goalkeeper_enum AS ENUM (
                'Claim',
                'Clear',
                'Collected Twice',
                'Fail',
                'In Play',
                'In Play Danger',
                'In Play Safe',
                'No Touch',
                'Saved Twice',
                'Success',
                'Touched In',
                'Touched Out',
                'Won',
                'Success In Play',
                'Success Out',
                'Lost In Play',
                'Lost Out',
                'Punched out'
            )
            """
        )

        cursor.execute("DROP TYPE IF EXISTS outcome_interception_enum CASCADE")
        cursor.execute(
            "CREATE TYPE outcome_interception_enum AS ENUM ('Lost', 'Lost In Play', 'Lost Out', 'Success', 'Success In Play', 'Success Out', 'Won')"
        )

        cursor.execute("DROP TYPE IF EXISTS pass_height_enum CASCADE")
        cursor.execute(
            "CREATE TYPE pass_height_enum AS ENUM ('Ground Pass', 'Low Pass', 'High Pass')"
        )

        cursor.execute("DROP TYPE IF EXISTS pass_type_enum CASCADE")
        cursor.execute(
            "CREATE TYPE pass_type_enum AS ENUM ('Corner', 'Free Kick', 'Goal Kick', 'Interception', 'Kick Off', 'Recovery', 'Throw-in')"
        )

        cursor.execute("DROP TYPE IF EXISTS pass_outcome_enum CASCADE")
        cursor.execute(
            "CREATE TYPE pass_outcome_enum AS ENUM ('Incomplete', 'Injury Clearance', 'Out', 'Pass Offside', 'Unknown')"
        )

        cursor.execute("DROP TYPE IF EXISTS pass_technique_enum CASCADE")
        cursor.execute(
            "CREATE TYPE pass_technique_enum AS ENUM ('Inswinging', 'Outswinging', 'Straight', 'Through Ball')"
        )

        cursor.execute("DROP TYPE IF EXISTS shot_technique_enum CASCADE")
        cursor.execute(
            "CREATE TYPE shot_technique_enum AS ENUM ('Backheel', 'Diving Header', 'Half Volley', 'Lob', 'Normal', 'Overhead Kick', 'Volley')"
        )

        cursor.execute("DROP TYPE IF EXISTS shot_type_enum CASCADE")
        cursor.execute(
            "CREATE TYPE shot_type_enum AS ENUM ('Corner', 'Free Kick', 'Open Play', 'Penalty', 'Kick Off')"
        )

        cursor.execute("DROP TYPE IF EXISTS outcome_shot_enum CASCADE")
        cursor.execute(
            "CREATE TYPE outcome_shot_enum AS ENUM ('Blocked', 'Goal', 'Off T', 'Post', 'Saved', 'Wayward', 'Saved Off Target', 'Saved to Post')"
        )

        cursor.execute("DROP TYPE IF EXISTS outcome_substitution_enum CASCADE")
        cursor.execute(
            "CREATE TYPE outcome_substitution_enum AS ENUM ('Injury', 'Tactical')"
        )


def tables():
    with conn.cursor() as cursor:
        # COMPETITIONS
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS competitions (
                competition_id INTEGER PRIMARY KEY,
                country_name TEXT,
                competition_name TEXT NOT NULL,
                competition_gender TEXT NOT NULL,
                competition_youth BOOLEAN,
                competition_international BOOLEAN
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS seasons (
                season_id INTEGER PRIMARY KEY,
                season_name TEXT NOT NULL
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS competition_seasons (
                competition_id INTEGER REFERENCES competitions(competition_id),
                season_id INTEGER REFERENCES seasons(season_id),
                match_updated DATE NOT NULL,
                match_updated_360 DATE,
                match_available_360 DATE,
                match_available DATE NOT NULL,
                PRIMARY KEY (competition_id, season_id)
            )
        """
        )

        # MATCHES
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS teams (
                team_id INTEGER PRIMARY KEY,
                team_name TEXT NOT NULL,
                team_gender TEXT NOT NULL,
                team_group BOOLEAN,
                country TEXT NOT NULL
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS stadiums (
                stadium_id INTEGER PRIMARY KEY,
                stadium_name TEXT NOT NULL,
                country TEXT NOT NULL
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS referees (
                referee_id INTEGER PRIMARY KEY,
                referee_name TEXT NOT NULL,
                country TEXT NOT NULL
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS managers (
                manager_id INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                nickname TEXT,
                dob TEXT NOT NULL,
                country TEXT NOT NULL
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS matches (
                match_id INTEGER PRIMARY KEY,
                match_date TEXT NOT NULL,
                kick_off TEXT NOT NULL,
                competition_id INTEGER REFERENCES competitions(competition_id),
                season_id INTEGER REFERENCES seasons(season_id),
                home_team INTEGER REFERENCES teams(team_id),
                away_team INTEGER REFERENCES teams(team_id),
                home_score INTEGER NOT NULL,
                away_score INTEGER NOT NULL,
                match_status TEXT NOT NULL,
                match_status_360 TEXT NOT NULL,
                last_updated TEXT NOT NULL,
                last_updated_360 TEXT NOT NULL,
                match_week INTEGER NOT NULL,
                competition_stage TEXT NOT NULL,
                stadium INTEGER REFERENCES stadiums(stadium_id),
                referee INTEGER REFERENCES referees(referee_id)
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS managed_by (
                manager_id INTEGER REFERENCES managers(manager_id),
                match_id INTEGER REFERENCES matches(match_id),
                team_id INTEGER REFERENCES teams(team_id)
            )
        """
        )

        # LINEUPS
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS players (
                player_id INTEGER PRIMARY KEY,
                player_name TEXT NOT NULL,
                player_nickname TEXT,
                country TEXT NOT NULL
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS played_for (
                player_id INTEGER REFERENCES players(player_id),
                match_id INTEGER REFERENCES matches(match_id),
                team_id INTEGER REFERENCES teams(team_id),
                jersey_number INTEGER NOT NULL,
                PRIMARY KEY (player_id, match_id, team_id)
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS cards (
                player_id INTEGER REFERENCES players(player_id),
                match_id INTEGER REFERENCES matches(match_id),
                card_type card_enum NOT NULL,
                time TEXT NOT NULL,
                reason TEXT NOT NULL,
                period INTEGER NOT NULL
            )
        """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS played_position (
                player_id INTEGER REFERENCES players(player_id),
                match_id INTEGER REFERENCES matches(match_id),
                position positions_enum,
                from_ TEXT NOT NULL,
                to_ TEXT,
                from_period INTEGER NOT NULL,
                to_period INTEGER,
                start_reason TEXT NOT NULL,
                end_reason TEXT NOT NULL
            )
        """
        )

        # EVENTS
        for event in TABLES:
            cursor.execute("DROP TABLE IF EXISTS " + event)
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {event} (
                    match_id INTEGER REFERENCES matches(match_id),
                    {dict_to_table(TABLES[event])[:-2]},
                    PRIMARY KEY (match_id, id)
                ) PARTITION BY RANGE(match_id)
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {event}_la_liga_18_19_season PARTITION OF {event}
                FOR VALUES FROM (15946) TO (16318);
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {event}_la_liga_19_20_season PARTITION OF {event}
                FOR VALUES FROM (303377) TO (303732);
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {event}_la_liga_20_21_season PARTITION OF {event}
                FOR VALUES FROM (3764440) TO (3773696);
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {event}_premier_03_04_season PARTITION OF {event}
                FOR VALUES FROM (3749052) TO (3749643);
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {event}_rest PARTITION OF {event}
                DEFAULT;
                """
            )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tactical_shifts_lineup (
                match_id INTEGER,
                tactical_shift_id UUID,
                player_id INTEGER REFERENCES players(player_id),
                FOREIGN KEY (match_id, tactical_shift_id) REFERENCES tactical_shifts(match_id, id)
            )
        """
        )

        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS starting_xis_lineup (
                match_id INTEGER,
                starting_xis_id UUID,
                player_id INTEGER REFERENCES players(player_id),
                FOREIGN KEY (match_id, starting_xis_id) REFERENCES starting_xis(match_id, id)
            )
        """
        )

    conn.commit()


import json
import os

base = "./open-data/data"


def data():
    with conn.cursor() as cursor:
        competitions = set()
        seasons = set()
        for competition in json.load(open(f"{base}/competitions.json")):
            if competition["competition_id"] not in competitions:
                competitions.add(competition["competition_id"])
                cursor.execute(
                    """
                    INSERT INTO competitions (
                        competition_id,
                        country_name,
                        competition_name,
                        competition_gender,
                        competition_youth,
                        competition_international
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """,
                    (
                        competition["competition_id"],
                        competition["country_name"],
                        competition["competition_name"],
                        competition["competition_gender"],
                        competition["competition_youth"],
                        competition["competition_international"],
                    ),
                )
            if competition["season_id"] not in seasons:
                seasons.add(competition["season_id"])
                cursor.execute(
                    "INSERT INTO seasons (season_id, season_name) VALUES (%s, %s)",
                    (competition["season_id"], competition["season_name"]),
                )
            cursor.execute(
                "INSERT INTO competition_seasons (competition_id, season_id, match_updated, match_updated_360, match_available_360, match_available) VALUES (%s, %s, %s, %s, %s, %s)",
                (
                    competition["competition_id"],
                    competition["season_id"],
                    competition["match_updated"],
                    competition["match_updated_360"],
                    competition["match_available_360"],
                    competition["match_available"],
                ),
            )

        matches = []
        for competition in os.listdir(f"{base}/matches"):
            for season in os.listdir(f"{base}/matches/{competition}"):
                for match in json.load(open(f"{base}/matches/{competition}/{season}")):
                    match["competition"] = competition
                    match["season"] = season[: -len(".json")]
                    matches.append(match)

        teams = set()
        for match in matches:
            team = match["home_team"]
            if team["home_team_id"] not in teams:
                teams.add(team["home_team_id"])
                cursor.execute(
                    "INSERT INTO teams (team_id, team_name, team_gender, team_group, country) VALUES (%s, %s, %s, %s, %s)",
                    (
                        team["home_team_id"],
                        team["home_team_name"],
                        team["home_team_gender"],
                        team["home_team_group"],
                        team["country"]["name"],
                    ),
                )
            team = match["away_team"]
            if team["away_team_id"] not in teams:
                teams.add(team["away_team_id"])
                cursor.execute(
                    "INSERT INTO teams (team_id, team_name, team_gender, team_group, country) VALUES (%s, %s, %s, %s, %s)",
                    (
                        team["away_team_id"],
                        team["away_team_name"],
                        team["away_team_gender"],
                        team["away_team_group"],
                        team["country"]["name"],
                    ),
                )

        for match in matches:
            cursor.execute(
                "INSERT INTO matches (match_id, match_date, kick_off, competition_id, season_id, home_team, away_team, home_score, away_score, match_status, match_status_360, last_updated, last_updated_360, match_week, competition_stage) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (
                    match["match_id"],
                    match["match_date"],
                    match["kick_off"],
                    match["competition"],
                    match["season"],
                    match["home_team"]["home_team_id"],
                    match["away_team"]["away_team_id"],
                    match["home_score"],
                    match["away_score"],
                    match["match_status"],
                    match["match_status_360"],
                    match["last_updated"],
                    match["last_updated_360"],
                    match["match_week"],
                    match["competition_stage"]["name"],
                ),
            )

        stadiums = set()
        for match in matches:
            if "stadium" not in match:
                continue
            stad = match["stadium"]
            if stad["id"] not in stadiums:
                stadiums.add(stad["id"])
                cursor.execute(
                    "INSERT INTO stadiums (stadium_id, stadium_name, country) VALUES (%s, %s, %s)",
                    (stad["id"], stad["name"], stad["country"]["name"]),
                )

        refs = set()
        for match in matches:
            if "referee" not in match:
                continue
            ref = match["referee"]
            if ref["id"] not in refs:
                refs.add(ref["id"])
                cursor.execute(
                    "INSERT INTO referees (referee_id, referee_name, country) VALUES (%s, %s, %s)",
                    (ref["id"], ref["name"], ref["country"]["name"]),
                )

        managers = set()
        for match in matches:
            team = match["home_team"]
            for manager in team.get("managers", []):
                if manager["id"] not in managers:
                    managers.add(manager["id"])
                    cursor.execute(
                        "INSERT INTO managers (manager_id, name, nickname, dob, country) VALUES (%s, %s, %s, %s, %s)",
                        (
                            manager["id"],
                            manager["name"],
                            manager["nickname"],
                            manager["dob"],
                            manager["country"]["name"],
                        ),
                    )
                cursor.execute(
                    "INSERT INTO managed_by (manager_id, match_id, team_id) VALUES (%s, %s, %s)",
                    (manager["id"], match["match_id"], team["home_team_id"]),
                )
            team = match["away_team"]
            for manager in team.get("managers", []):
                if manager["id"] not in managers:
                    managers.add(manager["id"])
                    cursor.execute(
                        "INSERT INTO managers (manager_id, name, nickname, dob, country) VALUES (%s, %s, %s, %s, %s)",
                        (
                            manager["id"],
                            manager["name"],
                            manager["nickname"],
                            manager["dob"],
                            manager["country"]["name"],
                        ),
                    )
                cursor.execute(
                    "INSERT INTO managed_by (manager_id, match_id, team_id) VALUES (%s, %s, %s)",
                    (manager["id"], match["match_id"], team["away_team_id"]),
                )

        lineups = []
        for match in os.listdir(f"{base}/lineups"):
            match_id = match[: -len(".json")]
            for lineup in json.load(open(f"{base}/lineups/{match}")):
                lineup["match_id"] = match_id
                lineups.append(lineup)

        players = set()
        for lineup in lineups:
            for player in lineup["lineup"]:
                if player["player_id"] not in players:
                    players.add(player["player_id"])
                    cursor.execute(
                        "INSERT INTO players (player_id, player_name, player_nickname, country) VALUES (%s, %s, %s, %s)",
                        (
                            player["player_id"],
                            player["player_name"],
                            player["player_nickname"],
                            player["country"]["name"],
                        ),
                    )
                cursor.execute(
                    "INSERT INTO played_for (player_id, match_id, team_id, jersey_number) VALUES (%s, %s, %s, %s)",
                    (
                        player["player_id"],
                        lineup["match_id"],
                        lineup["team_id"],
                        player["jersey_number"],
                    ),
                )
                for card in player["cards"]:
                    cursor.execute(
                        "INSERT INTO cards (player_id, match_id, card_type, time, reason, period) VALUES (%s, %s, %s, %s, %s, %s)",
                        (
                            player["player_id"],
                            lineup["match_id"],
                            card["card_type"],
                            card["time"],
                            card["reason"],
                            card["period"],
                        ),
                    )
                for position in player["positions"]:
                    cursor.execute(
                        "INSERT INTO played_position (player_id, match_id, position, from_, to_, from_period, to_period, start_reason, end_reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                        (
                            player["player_id"],
                            lineup["match_id"],
                            position["position"],
                            position["from"],
                            position["to"],
                            position["from_period"],
                            position["to_period"],
                            position["start_reason"],
                            position["end_reason"],
                        ),
                    )

        parsers = {}
        for key, value in TABLES.items():
            parsers[key] = dict_to_parser(value)

        with conn.pipeline():
            for match in os.listdir(f"{base}/events"):
                match_id = match[: -len(".json")]
                for event in json.load(open(f"{base}/events/{match}")):
                    if event["type"]["name"] == "50/50":
                        table_name = "fifty_fifties"
                    elif event["type"]["name"] == "Bad Behaviour":
                        table_name = "bad_behaviours"
                    elif event["type"]["name"] == "Ball Receipt*":
                        table_name = "ball_receipts"
                    elif event["type"]["name"] == "Ball Recovery":
                        table_name = "ball_recoveries"
                    elif event["type"]["name"] == "Block":
                        table_name = "blocks"
                    elif event["type"]["name"] == "Carry":
                        table_name = "carries"
                    elif event["type"]["name"] == "Clearance":
                        table_name = "clearances"
                    elif event["type"]["name"] == "Dispossessed":
                        table_name = "dispossessions"
                    elif event["type"]["name"] == "Dribble":
                        table_name = "dribbles"
                    elif event["type"]["name"] == "Dribbled Past":
                        table_name = "dribbled_pasts"
                    elif event["type"]["name"] == "Duel":
                        table_name = "duels"
                    elif event["type"]["name"] == "Error":
                        table_name = "errors"
                    elif event["type"]["name"] == "Foul Committed":
                        table_name = "fouls_committed"
                    elif event["type"]["name"] == "Foul Won":
                        table_name = "fouls_won"
                    elif event["type"]["name"] == "Goal Keeper":
                        table_name = "goalkeepers"
                    elif event["type"]["name"] == "Half End":
                        table_name = "half_ends"
                    elif event["type"]["name"] == "Half Start":
                        table_name = "half_starts"
                    elif event["type"]["name"] == "Injury Stoppage":
                        table_name = "injury_stoppages"
                    elif event["type"]["name"] == "Interception":
                        table_name = "interceptions"
                    elif event["type"]["name"] == "Miscontrol":
                        table_name = "miscontrols"
                    elif event["type"]["name"] == "Offside":
                        table_name = "offsides"
                    elif event["type"]["name"] == "Own Goal Against":
                        table_name = "own_goals_against"
                    elif event["type"]["name"] == "Own Goal For":
                        table_name = "own_goals_for"
                    elif event["type"]["name"] == "Pass":
                        table_name = "passes"
                    elif event["type"]["name"] == "Player Off":
                        table_name = "player_offs"
                    elif event["type"]["name"] == "Player On":
                        table_name = "player_ons"
                    elif event["type"]["name"] == "Pressure":
                        table_name = "pressures"
                    elif event["type"]["name"] == "Referee Ball-Drop":
                        table_name = "referee_ball_drops"
                    elif event["type"]["name"] == "Shield":
                        table_name = "shields"
                    elif event["type"]["name"] == "Shot":
                        table_name = "shots"
                    elif event["type"]["name"] == "Starting XI":
                        table_name = "starting_xis"
                    elif event["type"]["name"] == "Substitution":
                        table_name = "substitutions"
                    elif event["type"]["name"] == "Tactical Shift":
                        table_name = "tactical_shifts"
                    else:
                        raise Exception(f"Unknown event type: {event['type']['name']}")
                    result = parsers[table_name](event)
                    try:
                        cursor.execute(
                            f"INSERT INTO {table_name}(match_id, {', '.join(result.keys())}) VALUES (%s, {', '.join(['%s'] * len(result))})",
                            [match_id, *result.values()],
                        )
                    except Exception as e:
                        print(e)
                        print(
                            f"INSERT INTO {table_name}(match_id, {', '.join(result.keys())}) VALUES ({', '.join(['%s'] * len(result))})"
                        )
                        print(list(result.values()))
                        exit(1)

                    if event["type"]["name"] == "Starting XI":
                        for player in event["tactics"]["lineup"]:
                            cursor.execute(
                                "INSERT INTO starting_xis_lineup(match_id, starting_xis_id, player_id) VALUES (%s, %s, %s)",
                                (match_id, event["id"], player["player"]["id"]),
                            )
                    elif event["type"]["name"] == "Tactical Shift":
                        for player in event["tactics"]["lineup"]:
                            cursor.execute(
                                "INSERT INTO tactical_shifts_lineup(match_id, tactical_shift_id, player_id) VALUES (%s, %s, %s)",
                                (match_id, event["id"], player["player"]["id"]),
                            )

    conn.commit()


def functions():
    with conn.cursor() as cursor:
        cursor.execute(
            """
            CREATE OR REPLACE FUNCTION get_competition_matches(s_name TEXT[], c_name TEXT[]) RETURNS TABLE(match_id INTEGER)
                AS 'SELECT match_id
                        FROM matches
                        WHERE season_id IN (SELECT season_id FROM seasons WHERE season_name = ANY(s_name) LIMIT array_length(s_name, 1))
                        AND competition_id IN (SELECT competition_id FROM competitions WHERE competition_name = ANY(c_name) LIMIT array_length(c_name, 1))'
                LANGUAGE SQL
                STABLE
                PARALLEL SAFE
                ROWS 38
        """
        )


def views():
    with conn.cursor() as cursor:
        cursor.execute(
            "CREATE OR REPLACE VIEW la_liga_single AS SELECT * FROM get_competition_matches(ARRAY['2020/2021'], ARRAY['La Liga'])"
        )
        cursor.execute(
            "CREATE OR REPLACE VIEW la_liga_triple AS SELECT * FROM get_competition_matches(ARRAY['2020/2021', '2019/2020', '2018/2019'], ARRAY['La Liga'])"
        )
        cursor.execute(
            "CREATE OR REPLACE VIEW premier_single AS SELECT * FROM get_competition_matches(ARRAY['2003/2004'], ARRAY['Premier League'])"
        )

    conn.commit()


def indexes():
    with conn.cursor() as cursor:
        cursor.execute("DROP INDEX IF EXISTS is_through_ball_pass_idx")

        cursor.execute(
            "CREATE INDEX is_through_ball_pass_idx ON passes((1)) WHERE pass_through_ball"
        )

        for event in TABLES:
            cursor.execute(f"DROP INDEX IF EXISTS {event}_match_id_idx")
            cursor.execute(
                f"CREATE INDEX {event}_match_id_idx ON {event} USING btree (match_id)"
            )

    conn.commit()


enums()
tables()
data()
functions()
views()
indexes()
