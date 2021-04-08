import pymongo 
import os
import requests
from bs4 import BeautifulSoup
import argparse
import time
import logging
import sys

def build_db_topic_connection():
    
    DB_USER = os.environ['DB_USER']
    DB_PASSWORD = os.environ['DB_USER_PASS']
    DB_NAME = os.environ['DB_NAME']
    DB_ENDPOINT = os.environ['DB_ENDPOINT']
    
    client = pymongo.MongoClient("mongodb://{}:{}@{}/{}".format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_NAME))
    db = client.saga

    collection = db.links

    return collection

def split_p_fields(content):
    p_fields = []
    soup = BeautifulSoup(content, 'html.parser')
    for p in soup.find_all('p'):
        p_fields.append(p.get_text())
    return p_fields

def calculate_words(p_fields):
    count = 0
    for field in p_fields:
        count += len(field.split())
    return count

def main():
    # Format the arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-size", help="Number of links to process per run")
    args = parser.parse_args()

    logging.Formatter.converter = time.gmtime
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s',level=logging.DEBUG)

    if not args.batch_size:
        logging.error("Requires --batch argument and parameter to determine batch size\nExiting...")
        sys.exit(0)
    else:
        logging.info("Using a batch size of " + args.batch_size)
        batch_size = int(args.batch_size)

    try:
        logging.info("Connecting to database")
        collection = build_db_topic_connection()
    except:
        logging.error("Can't contact database\nExiting")
        sys.exit(0)
    
    logging.info("Querying db for links without word count")
    query = {"p-words": None}
    links = list(collection.find(query).limit(batch_size))

    id_wordcount_map = []

    if len(links) > 0:
        for link in links:
            try:
                url = link['url']
                
                x = requests.get(url)
                p_fields = split_p_fields(x.text)

                words = calculate_words(p_fields)
                logging.info("Link " + url + " found " + str(words) + " words.")
                
                id_wordcount_map.append({
                    "_id": link["_id"],
                    "url": url,
                    "p-words": words
                })

            except:
                logging.warning("Failed to calculate words for link object: " + str(link))
                #Retry logic?
    else:
        logging.info("No links to update\nExiting")
        exit(0)

    for id_wordcount in id_wordcount_map:
        logging.info("Adding wordcount to link " + id_wordcount["url"] + " to DB")
        query = { "_id": id_wordcount["_id"] }
        value = { "$set": {"p-words": id_wordcount["p-words"]}}
        collection.update_one(query, value)

    logging.info("All processes complete")
    sys.exit(0)

if __name__ == "__main__":
    main()