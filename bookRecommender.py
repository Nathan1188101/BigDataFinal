from neo4j import GraphDatabase
import csv

# Database connection
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
#Fixed to not return duplicates but was just 2 print statements lol
query = """
        MATCH (b:Book),(r:Rating)
        WHERE b.title = $title
        RETURN b.title AS book, r.review_score AS Rating
        LIMIT 20;
        """

# Connect to the database
driver = GraphDatabase.driver(URI, auth=AUTH)

# Function to recommend books based on title
def recommend_books_by_title(title_name):
    title_name = f"{title_name}" #data comes back this format so its a small workaround so the user doesn't have to input [''] everytime
    print(title_name)
    with driver.session() as session:
        result = session.run(query, title=title_name)
        # Extract the books from the result. Logged originally after it kept failing and realized it returned an object
        books = [(record["book"], record["Rating"]) for record in result]
        print(books)
        return books

#try user input for the title name
title = input("Enter the books's title: ")
book_ratings = recommend_books_by_title(title)

# Display the the books and they're respective reviews.
if book_ratings:
    for book in book_ratings:
        print(f"Book: {book[0]}, Rating: {book[1]}")
else:
    print("No results found for this title.") 
    
driver.close()