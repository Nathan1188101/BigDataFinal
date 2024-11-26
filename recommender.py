from neo4j import GraphDatabase 
import os, sys
import csv

URI = "bolt://localhost:7687"
AUTH = ("final_bigdata", "supersecretpassword1")

with GraphDatabase.driver(URI, auth=AUTH) as driver: 
    with driver.session(database="neo4j") as session: 
        print("connected") #debugging purposes 
        with open (os.path.join(os.path.dirname(sys.argv[0]) +  "/Books_data_cleaned.csv"), mode = "r", encoding = 'uft-8') as file:
            reader = csv.reader(file, delimiter=",") #read the csv file and delimit by commas 

            next (reader) #skip the header row 

            for i, row in enumerate(reader): #iterate through the rows 
             #get all columns 
             title = row[0]
             author = row[1]
             publisher = row[2]
             categories = row[3] #genres 
             ratingsCount = row[4]

            #create a node for every book 
            












        ##query = """
       # MATCH (author:Author {name: $author}) <- [:WRITTEN_BY]-(book:Book)"""