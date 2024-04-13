import os
import json

matches = set()

for folder in os.listdir("open-data/data/matches"):
    for file in os.listdir(f"open-data/data/matches/{folder}"):
        with open(f"open-data/data/matches/{folder}/{file}", "r") as f:
            data = json.load(f)
            matches.update(x["match_id"] for x in data)

for file in os.listdir("open-data/data/events"):
    if int(file[: -len(".json")]) not in matches:
        os.remove(f"open-data/data/events/{file}")

for file in os.listdir("open-data/data/lineups"):
    if int(file[: -len(".json")]) not in matches:
        os.remove(f"open-data/data/lineups/{file}")

for file in os.listdir("open-data/data/three-sixty"):
    if int(file[: -len(".json")]) not in matches:
        os.remove(f"open-data/data/three-sixty/{file}")
