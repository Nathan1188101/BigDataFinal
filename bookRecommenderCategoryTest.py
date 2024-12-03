from neo4j import GraphDatabase
import csv

# Database connection
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
#Fixed to not return duplicates but was just 2 print statements lol

query = """
        MATCH (b:Book)
        WHERE b.categories = $category
        RETURN b.title AS book
        LIMIT 10;
        """

# Connect to the database
driver = GraphDatabase.driver(URI, auth=AUTH)

def gather_similar_books(category_name):
    category_name = f"['{category_name}']"
    print(category_name)
    with driver.session() as session:
        result = session.run(query, category=category_name)
        books = [(record["book"]) for record in result]
    return books

#try user input for the title name
title = input("Enter the category: ")
book_ratings = gather_similar_books(title)

# Display the the books and they're respective reviews.
if book_ratings:
    for book in book_ratings:
        print(f"Book: {book}")
else:
    print("No results found for this title.") 

driver.close()	#1910279