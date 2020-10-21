from pprint import pprint 
from DbConnector import DbConnector

import os
import datetime

class MongoProgram:

    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name):
        collection = self.db.create_collection(collection_name)    
        print('Created collection: ', collection)

    def insert_users(self, id , has_labels):
        doc = {
                "_id": id,
                "has_labels": has_labels,
                "Activities":  #Array of references to Activity documents
                    [
                    ]     
            }
        
        collection = self.db["User"]
        collection.insert_one(doc)

    def insert_activity(self, id,  user_id, transportation_mode, start_date_time, end_date_time):
        doc = {
                "_id": id,
                "user_id": user_id, #Reference to user/parent
                "transportation_mode": transportation_mode,
                "start_date_time": start_date_time,
                "end_date_time": end_date_time,
                "Trackpoints": #Array of references to Trackpoint documents
                    [
                    ] 
            }
        
        collection = self.db["Activity"]
        collection.insert_one(doc)

    def insert_trackPoint(self, trackpoints):

        collection = self.db["Trackpoint"]
        collection.insert_many(trackpoints)

    def update_documents_user(self, user_id, activity_ids):
        collection = self.db['User']
        newvalues = {"$set": {"Activities": activity_ids}}
        collection.update_one({'_id':user_id}, newvalues)

    def update_documents_activity(self, activity_id, trackpoint_ids):
        collection = self.db['Activity']
        newvalues = {"$set": {"Trackpoints": trackpoint_ids}}
        collection.update_one({'_id':activity_id}, newvalues)
     
    def fetch_documents(self, collection_name):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents: 
            pprint(doc)
        

    def drop_coll(self, collection_name):
        collection = self.db[collection_name]
        collection.drop()

        
    def show_coll(self):
        collections = self.client['test'].list_collection_names()
        print(collections)

    def getLabeledUsers(self, path):
        a_file = open(path, "r")

        list_of_lists = []
        for line in a_file:
            stripped_line = line.strip()
            line_list = stripped_line.split()
            list_of_lists.append(line_list)

        a_file.close()
        return list_of_lists
         


def main():
    program = None
    try:
        program = MongoProgram()
        
        #Fetching all labeled users from file
        labeled_list = program.getLabeledUsers("dataset\labeled_ids.txt")

        activityID = 1
        trackpointID = 1
        for (root,dirs, files) in os.walk('dataset\Data', topdown= 'true'): 
                print (root) 
                print (dirs) 

                activityIDs =[]
                userid = ""

                if (len(root) == 16): #In the xxx-user folder
                    userid = root[-3:] #Define userid matching folder in dataset
                    has_labels = "False"
                    print(userid)
                
                    for user in labeled_list:
                        if (userid == user[0]):
                            has_labels = "True"
                            break
                    
                    #Insert user to databse
                    program.insert_users(userid, has_labels)

                    #If user contains labeled activities
                    if (has_labels == "True"):
                        labels_path = root + "\\labels.txt"
                        labels_file = open (labels_path, "r")

                        activities = []

                        for line in labels_file:
                            activities.append(line)
                        
                        activities = activities [1:]

                        for activity in activities:
                            start_time = activity[:19]
                            end_time = activity[20:39]
                            transportation_mode = activity[40:]
                            
                            #Insert activity to database
                            program.insert_activity(str(activityID), userid, transportation_mode, start_time, end_time)
                            activityIDs.append(str(activityID))
                            
                            activityID += 1

                #All non-labeled activities and trackpoints
                if (len(root) == 27): #In the Trajectory folder
                    for file in files:
                        userid = root[13:16] #Define userid matching folder in dataset
                        
                        activity_file_path = "dataset\\Data\\" + userid + "\\Trajectory\\" + file

                        #Check if the file is too big
                        if ( sum(1 for line in open(activity_file_path)) < 2506):
                            activity_file = open(activity_file_path, "r")
                            trajectories = []
                            
                            #Extract data from file
                            for line in activity_file:
                                trajectories.append(line)
                            trajectories = trajectories[6:]
                            start_time = trajectories[0][-20:-10] + ' ' + trajectories[0][-9:-1]
                            end_time = trajectories[-1][-20:-10] + ' ' + trajectories[-1][-9:-1]
                            
                            #Insert activity to database
                            program.insert_activity(str(activityID), userid, "", start_time, end_time)
                            activityIDs.append(str(activityID))

                            trackpointIDs = []
                            #Preallocate list to store trackpoint-documents
                            trackpoints = []

                            for trajectory in trajectories :

                                #Extract data from file
                                data = trajectory.split(",")
                                lat = data[0]
                                lon = data[1]
                                altitude = data[3]
                                date_days = data[4]
                                date_time = data[5] + " " + data[6][:-1]
                                
                                #Make a document for one trackpoint
                                trackpoint = {
                                    "_id": trackpointID,
                                    "activity_id": activityID, #Reference to activity/parent
                                    "lat": lat,
                                    "lon": lon,
                                    "altitude": altitude,
                                    "date_days": date_days,
                                    "date_time": date_time,
                                }

                                trackpointIDs.append(str(trackpointID))
                                trackpoints.append(trackpoint) #Add trackpoint to list of documents
                                trackpointID += 1
                            
                            #Insert the whole list of trackpoint-documents for one activity simultainuously into database for efficiency
                            program.insert_trackPoint(trackpoints)
                            #Update activity with trackpoint references
                            program.update_documents_activity(str(activityID), trackpointIDs)
                            activityID += 1


                #Update User with activity references
                program.update_documents_user(userid, activityIDs)     
                print ('--------------------------------')

        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()


if __name__ == '__main__':
    main()
