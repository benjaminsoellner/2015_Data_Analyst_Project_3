from pymongo import MongoClient
from ggplot import *
import pymongo
import pprint
import pandas

def stats_users(osmnodesColl):
    return list(osmnodesColl.aggregate([
            {"$match": {"created.user": {"$exists": True}}},
            {"$group": {"_id": "$created.user", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))


def stats_amenities(osmnodesColl):
    return list(osmnodesColl.aggregate([
            {"$match": {"amenity": {"$exists": True}}},
            {"$group": {"_id": "$amenity", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))

def stats_leisure_activities(osmnodesColl):
    return list(osmnodesColl.aggregate([
            {"$match": {"leisure": {"$exists": True}}},
            {"$group": {"_id": "$leisure", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))

def stats_religions(osmnodesColl):
    return list(osmnodesColl.aggregate([
            {"$match": {"amenity":{"$in": ["place_of_worship","community_center"]}}},
            {"$group": {"_id": "$religion", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))

def stats_cuisines(osmnodesColl):
    return list(osmnodesColl.aggregate([
            {"$match": {"amenity": "restaurant"}},
            {"$group": {"_id": "$cuisine", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))

def stats_beers(osmnodesColl):
    return list(osmnodesColl.aggregate([
            {"$match": {"amenity": {"$in":["pub","bar","restaurant"]}}},
            {"$group": {"_id": "$brewery", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))

def stats_sports(osmnodesColl):
    return list(osmnodesColl.aggregate([
            {"$match": {"leisure": {"$in": ["sports_centre","stadium"]}}},
            {"$group": {"_id": "$sport", "count": {"$sum": 1}}},
            {"$sort": {"count": -1}}
        ]))

def stats_dances(osmnodesColl):
    return list(osmnodesColl.distinct("name", {"leisure": "dance"}))

def draw_graph(data, title, imageFileName):
    label_list = [row["_id"] for row in data]
    ids = pandas.Series(range(0,len(label_list)))
    counts = pandas.Series([row["count"] for row in data])
    df = pandas.DataFrame({"_id": ids, "count": counts})
    plot = ggplot(df, aes('_id', 'count')) + \
        geom_bar(stat='identity', fill='blue') + \
        xlab('Value') + \
        ylab('Instance') + \
        ggtitle(title) + \
        scale_x_discrete(labels=label_list,breaks=range(0,len(label_list))) + \
        theme(axis_text_x = element_text(angle = 45, hjust = 1))
    ggsave(imageFileName, plot)
    print "Saved plot of data to: " + imageFileName
    
def audit_stats_map(mongoServer, mongoPort, imageFileNamePattern):
    client = MongoClient(mongoServer + ":" + mongoPort)
    c = client.udacity.osmnodes
    print
    print "Users involved in creating the data:"
    r = stats_users(c)
    pprint.pprint(r)
    print
    print "Displaying types of amenities:"
    r = stats_amenities(c)
    pprint.pprint(r)
    draw_graph(r, "Types of amenities", imageFileNamePattern.format("stats_amenities.png"))
    print
    print "Displaying types of leisure activities:"
    r = stats_leisure_activities(c)
    pprint.pprint(r)
    draw_graph(r, "Types of leisure activities", imageFileNamePattern.format("stats_leisure_activities.png"))
    print
    print "Displaying prevalence of religions:"
    r = stats_religions(c)
    pprint.pprint(r)
    draw_graph(r, "Prevalence of relgions", imageFileNamePattern.format("stats_religions.png"))
    print
    print "Displaying types of cuisines in restaurants:"
    r = stats_cuisines(c)
    pprint.pprint(r)
    draw_graph(r, "Types of cuisines in restaurants", imageFileNamePattern.format("stats_cuisines.png"))
    print
    print "Displaying types of beers in bars and pubs:"
    r = stats_beers(c)
    pprint.pprint(r)
    draw_graph(r, "Types of beers in bars and pubs", imageFileNamePattern.format("stats_beers.png"))
    print
    print "Displaying types of sports in stadiums and sport centres:"
    r = stats_sports(c)
    pprint.pprint(r)
    draw_graph(r, "Types of sports in stadiums and sport centres", imageFileNamePattern.format("stats_sports.png"))
    print
    print "Where to dance in Dresden:"
    r = stats_dances(c)
    pprint.pprint(r)