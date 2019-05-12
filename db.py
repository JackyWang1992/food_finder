# Data pre-process from the huge json file provided by Yelp, extract AZ restaurants and CA as test
#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, json, re, io
import sqlite3
import pandas as pd

'''
Build database using sqlite3 api, and insert all the tables to the database
'''
def load_data():
    # to build the database
    if os.path.exists("yelp.db"):
        os.remove("yelp.db")
    # create a database connection.
    conn = sqlite3.connect("yelp.db")
    # create a cursor
    curr = conn.cursor()
    # send a pragma command to tell SQLite to check foreign key
    curr.execute("PRAGMA foreign_keys = ON;")

    # create tables
    curr.execute("CREATE TABLE BUSINESS (address TEXT ,attributes TEXT, business_id TEXT PRIMARY KEY, categories TEXT, city TEXT, hours TEXT, is_open INTEGER, latitude FLOAT , longitude FLOAT ,name TEXT,postal_code TEXT,review_count INTEGER ,stars FLOAT ,state TEXT)")
    curr.execute("CREATE TABLE REVIEW (business_id TEXT, cool INTEGER , date TEXT, funny INTEGER , review_id TEXT, stars INTEGER, review TEXT, useful INTEGER , user_id TEXT)")
    curr.execute("CREATE TABLE TIP (tip TEXT, date TEXT, compliment_count INTEGER, business_id TEXT, user_id TEXT)")

    # commit
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
            # for dictionary object, convert them to string
            i[1] = str(i[1]) if isinstance(i[1], dict) else "None"
            i[3] = str(i[3]) if isinstance(i[3], dict) else "None"
            i[5] = str(i[5]) if isinstance(i[5], dict) else "None"
        # insert into the table
        curr.executemany("INSERT INTO BUSINESS (address,attributes, business_id, categories, city, hours, is_open, latitude, longitude, name ,postal_code,review_count,stars,state) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?);", business_t)
        # commit change
        conn.commit()
        # to stop the while loop if all file been read
        try:
            buffer = next(business_reader)
        except:
            break
    business_reader.close()
    print("finish business")

    # read tip.json, same as above
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
    # finish reading all tables
    conn.close()

'''
Merge all three tables, and get the information we need for AZ restaurant search.
'''
def merge():
    # output to result.json
    result = open("result.json", 'w')
    conn = sqlite3.connect("yelp1.db")
    curr = conn.cursor()

    # merge BUSINESS + REVIEW
    # Yelp's data has all categories as None or All, so we figured out that all restaurant has the 'RestaurantsAttire'
    # in the Attributes, we use that to distinguish restaurant from all other businesses.
    # For reviews, we concatenate all reviews from one restaurant as one entry to prevent duplicate results.
    curr.execute(
        "SELECT BUSINESS.business_id,BUSINESS.name, BUSINESS.address, BUSINESS.attributes, BUSINESS.categories, BUSINESS.city, BUSINESS.hours, BUSINESS.postal_code ,BUSINESS.review_count ,BUSINESS.stars ,BUSINESS.state"
        "group_concat(REVIEW.review, '\n\n\n\n\n\n'), avg(REVIEW.useful), avg(REVIEW.cool), avg(REVIEW.funny)"
        " FROM BUSINESS, REVIEW "
        " WHERE BUSINESS.business_id = REVIEW.business_id "
        " AND BUSINESS.state = 'AZ'"
        " AND BUSINESS.attributes LIKE '%RestaurantsAttire%'"
        " GROUP BY BUSINESS.business_id")

    # build training set for naive bayes
    # curr.execute(
    #     "SELECT REVIEW.review, "
    #     " CASE"
    #     " WHEN REVIEW.stars <= 3 THEN 'neg'"
    #     " WHEN REVIEW.stars >3 THEN 'pos'"
    #     " END as sentiment"
    #     " FROM BUSINESS, REVIEW"
    #     " WHERE BUSINESS.business_id = REVIEW.business_id "
    #     " AND BUSINESS.state IN ('CA', 'NY', 'IL')"
    #     " AND BUSINESS.attributes LIKE '%RestaurantsAttire%'")

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

        # build training test for naive bayes classifier
        # dd['review'] = content[0]
        # dd['stars'] = content[1]

        final[i] = dd
        i += 1
    # write to json file
    json.dump(final, result)
    result.close()


if __name__ == "__main__":
    # loading file takes too long, so comment it out for now.
    # load_data()
    merge()