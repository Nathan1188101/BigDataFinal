import pandas as pd
import os, sys, csv
from neo4j import GraphDatabase 



URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
#take in the data and clean it up from all the data we do not need to load it quicker later.
file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Books_rating.csv'))

df = pd.read_csv(file_path)

columns_to_drop = ['price', 'review/text', 'review/summary', 'review/time', 'review/helpfullness']
df_final = df.drop(columns=columns_to_drop, errors='ignore')

output_file_path = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Books_rating_cleaned.csv'))
df_final.to_csv(output_file_path, index=False)


file_path2 = os.path.abspath(os.path.join(os.path.dirname(__file__), 'books_data.csv'))

df2 = pd.read_csv(file_path2)
columns_to_drop2 = ['description','image','previewLink','publishedDate','infoLink']
df_final2 = df2.drop(columns=columns_to_drop2, errors='ignore')

output_file_path2 = os.path.abspath(os.path.join(os.path.dirname(__file__), 'Books_data_cleaned.csv'))
df_final2.to_csv(output_file_path2, index=False)

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
        print("Finished processing all rows from The cleaned books rating file")

        with open (os.path.join(os.path.dirname(sys.argv[0]) +  "/Books_data_cleaned.csv"), mode = "r", encoding = 'UTF-8') as file:
                reader = csv.reader(file, delimiter=",") #read the csv file and delimit by commas 

                next (reader) #skip the header row 

                for i, row in enumerate(reader): #iterate through the rows

                    if i >= 10000:
                        print("10,000 rows processed, stopping now...")
                        break 

                    #get all columns 
                    title = row[0]
                    author = row[1]
                    publisher = row[2]
                    categories = row[3] #genres 
                    ratingsCount = row[4]

                    #create a node for every book, creating a node with label Book and properties title, author, publisher, categories, ratingsCount
                    session.run("""
                        CREATE (b:Book { 
                        title: $title,
                        author: $author,
                        publisher: $publisher,
                        categories: $categories,
                        ratingsCount: $ratingsCount
                        })""",
                        title=title, 
                        author=author,
                        publisher=publisher,
                        categories=categories,
                        ratingsCount=int(ratingsCount) if ratingsCount.isdigit() else None #if ratings count is not a number then set it to None
                        )
            
                    #progress indication
                    if i %  100 == 0:
                        print(f"Processed {i} rows")
        print("Finished processing all rows from The cleaned books data file")

driver.close()
