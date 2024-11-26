from neo4j import GraphDatabase 
import os, sys
import csv

URI = "bolt://localhost:7687"
AUTH = ("final_bigdata", "supersecretpassword1")

with GraphDatabase.driver(URI, auth=AUTH) as driver: 
    with driver.session(database="neo4j") as session: 
        print("connected")