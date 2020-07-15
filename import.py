import pymongo
from matplotlib import path

connection = pymongo.MongoClient("localhost", 27017)
collection = connection.Vessels_Position.vessels_smart_key

p1 = path.Path([(-10, 48.3689), (-10, 51), (-4.4942, 51), (-4.4942, 48.3689)])
p2 = path.Path([(-4.4942, 48.3689), (-4.4942, 51), (0, 51), (0, 48.3689)])
p3 = path.Path([(-10, 45), (-10, 48.3689), (-4.4942, 48.3689), (-4.4942, 45)])
p4 = path.Path([(-4.4942, 45), (-4.4942, 48.3689), (0, 48.3689), (0, 45)])

def import_data():
    with open('nari_dynamic.csv') as f:
        count = 0
        for line in f.readlines()[1:]:
            count +=1
            fields = line.split(",")
            doc_id = str(count)
            mmsi = fields[0]
            longitude = float(fields[6])
            latitude = float(fields[7])
            location = {"type": "Point", "coordinates": [longitude, latitude]}
            time = fields[8]
            if p1.contains_points([(longitude, latitude)]):
                collection.insert_one({"_id": "ObjectId(" + doc_id + ")", 
                                       "MMSI": mmsi,
                                       "geokey": 1,
                                       "location": location,
                                       "time": time})
            if p2.contains_points([(longitude, latitude)]):
                collection.insert_one({"_id": "ObjectId(" + doc_id + ")", 
                                       "MMSI": mmsi,
                                       "geokey": 2,
                                       "location": location,
                                       "time": time})
            if p3.contains_points([(longitude, latitude)]):
                collection.insert_one({"_id": "ObjectId(" + doc_id + ")", 
                                       "MMSI": mmsi,
                                       "geokey": 3,
                                       "location": location,
                                       "time": time})
            if p4.contains_points([(longitude, latitude)]):
                collection.insert_one({"_id": "ObjectId(" + doc_id + ")", 
                                       "MMSI": mmsi, 
                                       "geokey": 4,
                                       "location": location,
                                       "time": time})

import_data()
collection.create_index([("location", pymongo.GEOSPHERE)])