from neo4j import GraphDatabase 
import os, sys
import csv

URI = "bolt://localhost:7687"
AUTH = ("neo4j", "supersecretpassword1")

def main():
    with GraphDatabase.driver(URI, auth=AUTH) as driver: 
        with driver.session(database="neo4j") as session: 
            print("connected") #debugging purposes 
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

                #calling to the author book connection function
                print("Creating author to book connections now...")
                session.execute_write(author_book_connections)

                #calling the category book connections function 
                print("Creating category connections now...")
                session.execute_write(category_book_connections)

                print("-----------------All completed------------------")
                        

#tx is used to run the transaction (it executes neo4j queries)
def author_book_connections(tx):
    #match all books with the same author and create a relationship between them 
    result = tx.run(
        """
        MATCH (b1:Book), (b2:Book)
        WHERE b1.author = b2.author 
            AND b1.author IS NOT NULL 
            AND b1.author <> "" 
            AND id(b1) < id(b2)
        MERGE (b1)-[:SAME_AUTHOR]->(b2)
        RETURN b1.title AS book1, b2.title AS book2;
        """
        #need to ignore no author books 
        )
    for record in result:
        print(f"Connected {record['book1']} with {record['book2']} by the same author")

#REMEMBER I think we need to increase db memory in order to execute this, we keep getting errors where we run out
def category_book_connections(tx):
    #matching all the books with the same category and creating an edge between them 
    result = tx.run(
        """
        MATCH(b1:Book)
        WHERE b1.categories IS NOT NULL AND b1.categories <> ""
        WITH b1
        MATCH (b2:Book)
        WHERE b1.categories = b2.categories AND b1.title < b2.title
        MERGE (b1)-[:SAME_CATEGORY]->(b2)
        RETURN b1.title AS book1, b2.title AS book2;
        """
    )
    for record in result: 
        print(f"Connected {record['book1']} with {record['book2']} by the same category")

if __name__ == "__main__":
    main()
    