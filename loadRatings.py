from neo4j import GraphDatabase 
import os, sys
import csv

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")

with GraphDatabase.driver(URI, auth=AUTH) as driver: 
    with driver.session(database="neo4j") as session: 
        print("connected") #debugging purposes 
            #create a node for every book, creating a node with label Book and properties title, author, publisher, categories, ratingsCount
        with open (os.path.join(os.path.dirname(sys.argv[0]) +  "/Books_rating_cleaned.csv"), mode = "r", encoding = 'UTF-8') as file:
            reader = csv.reader(file, delimiter=",") #read the csv file and delimit by commas 

            next (reader) #skip the header row 

            for i, row in enumerate(reader): #iterate through the rows 
                #get all columns 
                id = row[0]
                title = row[1]
                price = row[2]
                user_id = row[3] #genres 
                profileName = row[4]
                review_helpfulness = row[5]
                review_score = row[6]
                session.run("""
                                CREATE (r:Rating { 
                                id: $id,
                                title: $title,
                                price: $price,
                                user_id: $user_id,
                                profileName: $profileName,
                                review_helpfulness: $review_helpfulness,
                                review_score: $review_score
                                })""",
                                id=id,
                                title=title,
                                price=price,
                                user_id=user_id,
                                profileName=profileName,
                                review_helpfulness=review_helpfulness,
                                review_score=review_score 
                                )
                if i %  10000 == 0:
                    print(f"Processed {i} rows")
            
                            
            