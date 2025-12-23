#!/usr/bin/env python
# * Libraries
import os
import pandas as pd


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
mypd.columns = ["gamedate", "name", "roll1", "roll2", "roll3", "roll4",
                "roll5", "roll6", "fullh", "same3", "same4", "strsm", "strlg",
                "chanc", "yathz"]


# * Split 'Vincent' and 'Stefanie' data
vkpd = mypd[mypd['name'].isin(['Vincent'])]
sbpd = mypd[mypd['name'].isin(['Stefanie'])]


# * Generate unique identifiers per game
# For matching, the games need to have unique identifiers. The date (gamedate)
# variable is not unique, as the time is not entered. So, I will need to create
# a new ID that adds time, and unique times per game.

# ** Function to create a unique gamedate ID from existing gamedate ID
# def genuniq(ipd):

#     # *** Unique game dates
#     ugames = ipd.loc[:,"gamedate"].unique()

#     # *** Loop over unique values
#     for u in ugames:

#         # Generate a range of unique consequtive numbers (the hours) for each
#         # row for a given date. So, subset the df where the date is u, then
#         # use this in a range function to generate the list of consequtive
#         # numbers.
#         ipd.loc["myhours"] = range(ipd.loc[ipd['gamedate'].isin([u])].shape[0])

#         # Add a column with consequtive numbers in for the rows matching the date
#         ipd.loc[ipd['gamedate'].isin([u]), 'hour'] = myhours

#     # *** Merge the hours with the date
#     # Convert to date/time column
#     ipd.loc[:,"gamedate"] = pd.to_datetime(ipd.loc[:,"gamedate"])

#     # Column
#     ipd.loc[:,"gamedate"] = pd.to_datetime({
#         "year": ipd.gamedate.dt.year,
#         "month": ipd.gamedate.dt.month,
#         "day": ipd.gamedate.dt.day,
#         "hour": ipd.gamedate.myhours,
#         "minute": 0,
#         "second": 0
#     })

#     # *** Drop myhours column
#     ipd = ipd.drop(columns = ["myhours"])

#     # *** Return data frame object
#     return(ipd)

# ** Function to create a unique gamedate ID from existing gamedate ID
def genuniq(ipd):
    # *** Make a copy
    ipd = ipd.copy()

    # *** Create myhours by grouping by date and counting from 0
    # The groupby method runs the 'cumcount' (cumulative count) by blocks of
    # unique gamedate values.
    ipd["myhours"] = ipd.groupby("gamedate").cumcount()

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


vkpd = genuniq(vkpd)
sbpd = genuniq(sbpd)
