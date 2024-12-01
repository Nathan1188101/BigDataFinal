from neo4j import GraphDatabase
import csv

# Database connection
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")
#matches books on author but has 1 duplicate result each time, idk how to fix that yet.
query = """
        MATCH (b1:Book), (b2:Book)
        WHERE b1.author = $author
            AND b2.author = $author
            AND b1.author IS NOT NULL
            AND b1.author <> "" 
            AND b1 <> b2
        WITH DISTINCT b1, b2  // Ensure each book pair is unique
        WHERE b1.title < b2.title  // Ensure each pair is unique by ordering
        RETURN b1.title AS book1, b2.title AS book2
        LIMIT 20;
        """

# Connect to the database
driver = GraphDatabase.driver(URI, auth=AUTH)

# Function to recommend books based on author
def recommend_books_by_author(author_name):
    author_name = f"['{author_name}']" #data comes back this format so its a small workaround so the user doesn't have to input [''] everytime
    print(author_name)
    with driver.session() as session:
        result = session.run(query, author=author_name)
        # Extract the books from the result. Logged originally after it kept failing and realized it returned an object
        books = [(record["book1"], record["book2"]) for record in result]
        return books

#try user input for the author name
author = input("Enter the author's name: ")
recommended_books = recommend_books_by_author(author)

# Display the the books based on our authors name.
if recommended_books:
    for book in recommended_books:
        print(f"Book 1: {book[0]}, Book 2: {book[1]}")
else:
    print("No results found for this author.") #happens alot Books_data should have its spaces removed in author for easier checking, may think about this later but reloading the nodes is a pain


# Save Our Results if needed
with open(f'{author}_recommended_books.csv', mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    writer.writerow(["Book 1", "Book 2"])
    
    for book in recommended_books:
        print(f"Book 1: {book[0]}, Book 2: {book[1]}")
        writer.writerow(book)

# Close the driver when done
driver.close()