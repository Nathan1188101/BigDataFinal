from neo4j import GraphDatabase
import csv

# Database connection
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
#Fixed to not return duplicates but was just 2 print statements lol

query = """
        MATCH (b:Book), (b2:Book)
        WHERE $category IN b.categories
        AND $category IN b2.categories
        AND b.title <> b2.title
        RETURN b.title AS book, b2.title AS book2
        LIMIT 20;
        """

# Connect to the database
driver = GraphDatabase.driver(URI, auth=AUTH)

def gather_similar_books(category_name):
    category_name = f"['{category_name}']"
    print(category_name)
    with driver.session() as session:

        result = session.run(query, category=category_name)
        

        books = [(record["book"], record["book2"]) for record in result]
    return books




#try user input for the title name
title = input("Enter the category: ")
book_ratings = gather_similar_books(title)

# Display the the books and they're respective reviews.
if book_ratings:
    for book in book_ratings:
        print(f"Book: {book[0]}, Shares category With: {book[1]}")
else:
    print("No results found for this title.") 

driver.close()