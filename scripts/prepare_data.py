#!/usr/bin/env python
# * Libraries
import os
import pandas as pd
import sqlite3
from sqlalchemy import create_engine, Table, Column, Integer, String, Text, DateTime, MetaData


# * Environment
ifile = "/datadisk/Syncthing/Temporary/Yahtzee_20240825.xlsx"
odir = "/datadisk/Private/Python/20251222_Yathzee/database"
try:
    os.makedirs(odir)
except FileExistsError as e:
    print("Folder already exists!")


# * Read in database
mypd = pd.read_excel(
    ifile,
    sheet_name = "Score_formulier",
    usecols = "C:IN",
    skiprows = 1,
    nrows = 19,
    header = None
)


# * Drop computed values
# (total, bonus)
mypd = mypd.drop(mypd.index[8:12])


# * Transpose table
mypd = mypd.transpose()


# * Add column names
mypd.columns = [
    "gamedate", "name", "roll1", "roll2", "roll3", "roll4", "roll5", "roll6",
    "fullh", "same3", "same4", "strsm", "strlg", "chanc", "yathz"
]


# * Generate unique identifiers per game
# For matching, the games need to have unique identifiers. The date (gamedate)
# variable is not unique, as the time is not entered. So, I will need to create
# a new ID that adds time, and unique times per game.

# ** Function to create a unique gamedate ID from existing gamedate ID
def genuniq(ipd):
    # *** Make a copy
    ipd = ipd.copy()

    # *** Create myhours by grouping by date and counting from 0
    # The groupby method runs the 'cumcount' (cumulative count) by blocks of
    # unique gamedate values.
    ipd["myhours"] = ipd.groupby(["name", "gamedate"]).cumcount()

    # *** Convert gamedate to datetime type variable
    ipd["gamedate"] = pd.to_datetime(ipd["gamedate"])

    # *** Merge the hours with the date
    ipd["gamedate"] = pd.to_datetime({
        "year":   ipd["gamedate"].dt.year,
        "month":  ipd["gamedate"].dt.month,
        "day":    ipd["gamedate"].dt.day,
        "hour":   ipd["myhours"],
        "minute": 0,
        "second": 0,
    })

    return ipd.drop(columns=["myhours"])


# ** Apply the function
mypd = genuniq(mypd)


# * Prepare the table for sql
# Convert the gamedate and player names to foreign keys
mypd_scores = mypd.copy()

# ** Store column names without 'gamedata' and 'name'
colheads = ["game_id", "player_id"]
colheads.extend(mypd_scores.columns[2:].tolist())

# ** Generate foreign key column for players
mypd_scores["player_id"] = mypd_scores['name'].map({"Stefanie": 0, "Vincent": 1})

# ** Generate foreign key column for games
mypd_scores["game_id"] = mypd_scores.groupby(["gamedate"]).ngroup()

# ** Drop the gamedate and name variables and order columns
mypd_scores = mypd_scores[colheads]


# * Create players table
players_data = {'player_id': [0, 1], 'name': ["Stefanie", "Vincent"]}
mypd_players = pd.DataFrame(players_data)


# * Create games table
mypd_games = pd.DataFrame(mypd["gamedate"].unique())
mypd_games.columns = ["datetime"]
mypd_games["game_id"] = range(1, len(mypd_games)+1)
mypd_games = mypd_games[["game_id", "datetime"]]


# * Store the tables into an sqlite database
# ** Open the database
engine = create_engine("sqlite:///" + odir + "/gamedata.sqlite")

# ** Scores table
# *** Meta data (empty object)
metadata = MetaData()

# *** For each table, set the column flags
scores = Table(
    "scores",
    metadata,
    Column("game_id", Integer, primary_key=False, nullable=False),
    Column("player_id", Integer, primary_key=False, nullable=False),
    Column("roll1", Integer),
    Column("roll2", Integer),
    Column("roll3", Integer),
    Column("roll4", Integer),
    Column("roll5", Integer),
    Column("roll6", Integer),
    Column("fullh", Integer),
    Column("same3", Integer),
    Column("same4", Integer),
    Column("strsm", Integer),
    Column("strlg", Integer),
    Column("chanc", Integer),
    Column("yathz", Integer),
)

scores = Table(
    "players",
    metadata,
    Column("player_id", Integer, primary_key=True, nullable=False),
    Column("name", Text, primary_key=False, nullable=False),
)

scores = Table(
    "games",
    metadata,
    Column("game_id", Integer, primary_key=True, nullable=False),
    Column("datetime", DateTime, primary_key=False, nullable=False),
)

# *** Drop all tables and create new tables in the database
metadata.drop_all(engine)
metadata.create_all(engine)

# *** Write all tables to the database
mypd_scores.to_sql(
    "scores",
    engine,
    if_exists="append",
    index=False
)

mypd_players.to_sql(
    "players",
    engine,
    if_exists="append",
    index=False
)

mypd_games.to_sql(
    "games",
    engine,
    if_exists="append",
    index=False
)
