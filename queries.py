from DbConnector import DbConnector
from haversine import haversine
from tabulate import tabulate
from bson.objectid import ObjectId

import datetime

class MongoProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def show_all_tables_0(self):
        oneUser = self.db.User.find_one()
        oneActivity = self.db.Activity.find_one()
        oneTrackpoint = self.db.Trackpoint.find_one()
        print("*** One User *** ", oneUser)
        print("*** One Activity *** ", oneActivity)
        print("*** One Trackpoint *** ", oneTrackpoint)

    def count_all_entries_1(self):
        users = self.db.User.estimated_document_count()
        activities = self.db.Activity.estimated_document_count()
        trackpoints = self.db.Trackpoint.estimated_document_count()
        print("*** Users *** ", users)
        print("*** Activites *** ", activities)
        print("*** Trackpoints *** ", trackpoints)

    def average_activities_2(self):
        users = self.db.User.estimated_document_count()
        activities = self.db.Activity.estimated_document_count()
        print("*** Average activities per user *** ")
        print(activities/users)

    def top_20_users_3(self):
        users = self.db.User.aggregate(
            [
                {"$project": {"activitiy_count": {"$size": "$Activities"}}}, {"$sort": {"activitiy_count": -1}}, {"$limit": 20}
            ]
        )
        print("*** Top 20 users with highest no. of Activities ***")
        for user in users:
            print(user)

    def users_riding_taxi_4(self):
        users = self.db.Activity.aggregate ([
            {
                "$match": {"transportation_mode": "taxi"}
            },
            {
                "$group": {"_id": "$user_id"}
            }
        ]
        )
        print("*** Users that have rode taxi ***")
        for user in users:
            print(user)

    def transportation_count_not_blank_5(self):
        modes = self.db.Activity.aggregate([
            {
                "$match": {"transportation_mode": {"$ne": ""}}
            },
            {
                "$group": {
                    "_id": "$transportation_mode",
                    "count": {"$sum": 1}
                }
            }
        ]
        )

        print("*** Count of each transportation mode ***")
        for mode in modes:
            print(mode)

    def year_with_most_activities_6(self):
        year = self.db.Activity.aggregate([
            {"$group": {"_id": {"$year": {"$dateFromString": {"dateString": "$start_date_time"}}},
                        "numberOfActivities": {"$sum": 1}}},
            {"$sort": {"numberOfActivities": -1}},
            {"$limit": 1}
        ])
        print("*** Year with most Activities ***")
        for yr in year:
            print(yr)

    def year_with_most_recorded_hours_6a(self):
        years = self.db.Activity.aggregate([{"$group": {
            "_id": {"$year": {"$dateFromString": {"dateString": "$start_date_time"}}}, "totalHours": {"$sum": {
                "$divide": [{"$subtract": [
                    {"$dateFromString": {"dateString": "$end_date_time"}},
                    {"$dateFromString": {"dateString": "$start_date_time"}}
                ]}, 60 * 1000 * 60]}}}}, {"$sort": {"totalHours": -1}}, {"$limit": 1}])

        print("*** Year with most hours recorded ***")
        for year in years:
            print(year)

    def total_distance_walked_by_user_112_in_2008_7(self):

        user = self.db.Activity.aggregate([
            {
                "$match": {
                    "user_id": "112",
                    "transportation_mode": "walk",
                    "start_date_time": {"$gte": "2008-01-01T00:00:00Z"},
                    "end_date_time": {"$lte": "2008-12-31T23:59:59Z"}
                }
            }
        ])

        activities = []
        for act in user:
            activities.append(act)

        print("Total activities gathered: %d" % len(activities))
        coords = []
        distances = []

        for a in activities:
            trackpoints = a["Trackpoints"]

        print(trackpoints)

        for point in trackpoints:
            print(point)
            obj = ObjectId(point)
            print(repr(obj))

            #pt = self.db.Trackpoint.find_one({"_id": obj})
            #print(pt)

#        for t in trackpoints:
#            coords.append((t["latitude"], t["longitude"]))
        i = 1
        while i < len(coords):
            distances.append(haversine(coords[i - 1], coords[i]))
            i = i + 1

        s = sum(distances)
        print("Total distance walked by user 112 in 2008: %f kilometers." % s)

def main():
    program = None
    try:
        program = MongoProgram()

        program.show_all_tables_0()
        print("\n----------------------------------------------------------------------------\n")
        program.count_all_entries_1()
        print("\n----------------------------------------------------------------------------\n")
        program.average_activities_2()
        print("\n----------------------------------------------------------------------------\n")
        program.top_20_users_3()
        print("\n----------------------------------------------------------------------------\n")
        program.users_riding_taxi_4()
        print("\n----------------------------------------------------------------------------\n")
        program.transportation_count_not_blank_5()
        print("\n----------------------------------------------------------------------------\n")
        program.year_with_most_activities_6()
        print("\n----------------------------------------------------------------------------\n")
        program.year_with_most_recorded_hours_6a()
        print("\n----------------------------------------------------------------------------\n")
        #program.total_distance_walked_by_user_112_in_2008_7()

    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()



if __name__ == '__main__':
    main()

