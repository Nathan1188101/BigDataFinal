#bookrecommendercategorytest.py is the outer, while book recommender is the inner 

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
        LIMIT 20;
        """

#this will be for ratings 
query2 = """
        MATCH (b:Book),(r:Rating)
        WHERE b.title = $title AND r.title = $title
        RETURN b.title AS book, r.review_score AS Rating
        LIMIT 20;
        """

# Connect to the database
driver = GraphDatabase.driver(URI, auth=AUTH)

#function to gather similar books based on category, will figure out how to make this a more dynamic function so the user can choose between author or category etc. 
def gather_similar_books(category_name):

    category_name = f"['{category_name}']"


    with driver.session() as session:
        result = session.run(query, category=category_name)
        books = [(record["book"]) for record in result]

    return books

#try user input for the title name
user_input = input("Enter the category: ")
gathered_books = gather_similar_books(user_input) #calls the function and passes the users input  


#before displaying books we want to use the ratings calculations to determine which books are worth displaying. 
def return_score(title_name):

    title_name = f"{title_name}" 

    with driver.session() as session:

        result = session.run(query2, title=title_name)

        # Extract the books from the result. Logged originally after it kept failing and realized it returned an object
        scores = [(record["Rating"]) for record in result]
        score = 0.0
        for book in scores:
            score = score + float(book[0])

        score = score/len(scores)
        return score

# Display the the books
if gathered_books:
    i = 0
    for book in gathered_books:
        
        if i > 4:
            exit()

        else:
            score = return_score(book) #calls the function and passes the book title
            if score >= 4.0:
                i += 1
                print(f"Book: {book} with rating: {score:.2f}") #rounding the score to 2 decimal places
            else:
                continue

else:
    print("No results found for this title.")  

#closes the connection to the database
driver.close()