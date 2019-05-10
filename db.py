#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sqlite3
import json
import pandas as pd
import re
import io
 
def load_data():
   
    if os.path.exists("yelp.db"):
        os.remove("yelp.db")

    # next, we will create a new SQLite database.
    # create a database connection.
    conn = sqlite3.connect("yelp.db")
    # create a cursor (this is like a single session)
    curr = conn.cursor()
    # send a pragma command to tell SQLite to check foreign key
    # constraints (it does not by default :( )
    curr.execute("PRAGMA foreign_keys = ON;")


    # create tables.
    curr.execute("CREATE TABLE BUSINESS (address TEXT ,attributes TEXT, business_id TEXT PRIMARY KEY, categories TEXT, city TEXT, hours TEXT, is_open INTEGER, latitude FLOAT , longitude FLOAT ,name TEXT,postal_code TEXT,review_count INTEGER ,stars FLOAT ,state TEXT)")
    curr.execute("CREATE TABLE REVIEW (business_id TEXT, cool INTEGER , date TEXT, funny INTEGER , review_id TEXT, stars INTEGER, review TEXT, useful INTEGER , user_id TEXT)")
    curr.execute("CREATE TABLE TIP (tip TEXT, date TEXT, compliment_count INTEGER, business_id TEXT, user_id TEXT)")

    # commit is like save -- if you don't do it, nothing is written.
    conn.commit()

    # json files are too large for memory, so read them chunk by chunk using panda reader
    buffer_size = io.DEFAULT_BUFFER_SIZE
    # to read business.json
    business_reader = pd.read_json("business.json", lines=True,chunksize=buffer_size)
    buffer = next(business_reader)
    while True:
        # convert it from dataframe to list of list
        business_t = list(map(list, buffer.values))
        for i in business_t:
            # for dictionary object, conver them to string
            i[1] = str(i[1]) if isinstance(i[1], dict) else "None"
            i[3] = str(i[3]) if isinstance(i[3], dict) else "None"
            i[5] = str(i[5]) if isinstance(i[5], dict) else "None"
        # insert into the table
        curr.executemany("INSERT INTO BUSINESS (address,attributes, business_id, categories, city, hours, is_open, latitude, longitude, name ,postal_code,review_count,stars,state) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);", business_t)
        # commit change
        conn.commit()
        # to stop the while loop
        try:
            buffer = next(business_reader)
        except:
            break
    business_reader.close()
    print("finish business")
    # read tip.json
    tip_reader = pd.read_json("tip.json", lines=True, chunksize=buffer_size, encoding='utf-8')
    buffer = next(tip_reader)
    while True:
        tip_t = list(map(list, buffer.values))
        for i in tip_t:
            i[2] = str(i[2]) if isinstance(i[2], pd._libs.tslibs.timestamps.Timestamp) else "None"
            i[3] = re.sub(r'[^\x00-\x7f]', '', i[3])
        curr.executemany("INSERT INTO TIP (business_id,compliment_count, date, tip, user_id ) VALUES (?,?,?, ?,?);",tip_t)
        conn.commit()
        try:
            buffer = next(tip_reader)
        except:
            break
    tip_reader.close()
    print("finish tip")
    # read review.json
    review_reader = pd.read_json("review.json", lines=True, chunksize=buffer_size)
    buffer = next(review_reader)
    while True:
        review_t = list(map(list, buffer.values))
        for i in review_t:
            i[2] = str(i[2]) if isinstance(i[2], pd._libs.tslibs.timestamps.Timestamp) else "None"
            i[6] = re.sub(r'[^\x00-\x7f]', '', i[6])
        curr.executemany("INSERT INTO REVIEW (business_id, cool, date, funny, review_id, stars, review, useful, user_id) VALUES (?,?,?, ?,?,?, ?,?,?);",review_t)
        conn.commit()
        try:
            buffer = next(review_reader)
        except:
            break
    review_reader.close()
    print("finish review")

    conn.close()

def merge():
    result = open("result2.json", 'w')
    conn = sqlite3.connect("yelp1.db")
    curr = conn.cursor()
    # BUSINESS + REVIEW
    curr.execute(
        "SELECT BUSINESS.business_id,BUSINESS.name, BUSINESS.address, BUSINESS.attributes, BUSINESS.categories, BUSINESS.city, BUSINESS.hours, BUSINESS.postal_code ,BUSINESS.review_count ,BUSINESS.stars ,BUSINESS.state,"
        "group_concat(REVIEW.review, '\n\n\n\n\n\n'), avg(REVIEW.useful), avg(REVIEW.cool), avg(REVIEW.funny), REVIEW.date"
        " FROM BUSINESS, REVIEW "
        " WHERE BUSINESS.business_id = REVIEW.business_id "
        " AND BUSINESS.state = 'AZ'"
        " AND BUSINESS.attributes LIKE '%RestaurantsAttire%'"
        " GROUP BY BUSINESS.business_id")

    # curr.execute(
        # "SELECT BUSINESS.business_id,BUSINESS.name, BUSINESS.address, BUSINESS.attributes, BUSINESS.categories, BUSINESS.city, BUSINESS.hours, BUSINESS.postal_code ,BUSINESS.review_count ,BUSINESS.stars ,BUSINESS.state,"
        # "REVIEW.review,REVIEW.useful, REVIEW.cool, REVIEW.funny, REVIEW.date, REVIEW.review_id"
        # " FROM BUSINESS, REVIEW"
        # " WHERE BUSINESS.business_id = REVIEW.business_id "
        # " AND BUSINESS.state = 'AZ'"
        # " AND BUSINESS.attributes LIKE '%RestaurantsAttire%'")
    # BUSINESS + TIP
    # curr.execute(
    #     "SELECT BUSINESS.business_id,BUSINESS.name, BUSINESS.address, BUSINESS.attributes, BUSINESS.categories, BUSINESS.city, BUSINESS.hours, BUSINESS.postal_code ,BUSINESS.review_count ,BUSINESS.stars ,BUSINESS.state,"
    #     "TIP.tip, TIP.date, TIP.compliment_count"
    #     " FROM BUSINESS, TIP"
    #     " WHERE BUSINESS.business_id = TIP.business_id "
    #     " AND BUSINESS.state = 'CA'")
    # print(curr)
    # write to json
    final = {}
    i = 0
    # convert to dict
    for content in curr.fetchall():
        dd = {}
        dd['business_id'] = content[0]
        dd['business_name'] = content[1]
        dd['address'] = content[2]
        dd['attributes'] = content[3]
        dd['categories'] = content[4]
        dd['city'] = content[5]
        dd['hours'] = content[6]
        dd['postal_code'] = content[7]
        dd['review_count'] = content[8]
        dd['stars'] = content[9]
        dd['state'] = content[10]
        dd['review'] = content[11]
        dd['useful'] = content[12]
        dd['cool'] = content[13]
        dd['funny'] = content[14]
        dd['date'] = content[15]
        # dd['review_id'] = content[16]
        final[i] = dd
        i += 1

    json.dump(final, result)
    result.close()


if __name__ == "__main__":
    # load_data()
    merge()