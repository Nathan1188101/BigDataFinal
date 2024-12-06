from neo4j import GraphDatabase
import os
import cherrypy
#https://docs.cherrypy.dev/en/latest/index.html
# Database connection
URI = "bolt://localhost:7687"
AUTH = ("neo4j", "password")

# Queries
query = """
        MATCH (b:Book)
        WHERE b.title CONTAINS $title 
        RETURN b.title AS book
        ORDER BY rand()
        LIMIT 20;
        """

query2 = """
        MATCH (b:Book),(r:Rating)
        WHERE b.title = $title AND r.title = $title
        RETURN b.title AS book, r.review_score AS Rating
        LIMIT 20;
        """

# Database functions
def gather_similar_books(title_name):
    print(title_name)
    with driver.session() as session:
        result = session.run(query, title=title_name)
        books = [record["book"] for record in result]
    
    return books

def return_score(title_name):
    with driver.session() as session:
        result = session.run(query2, title=title_name)

        # Extract ratings scores from the query
        scores = [(record["Rating"]) for record in result]
        score = 0.0
        for book in scores:
            score = score + float(book[0])

        score = score/len(scores)
        return score

# Connect to the database
driver = GraphDatabase.driver(URI, auth=AUTH)

class DisplayResults:
    @cherrypy.expose
    # cherrypy to create a simple input form to gather the title.
    def index(self):
        return """
                <div style="display: flex; justify-content: center; padding: 20px;">
                    <head> 
                        <link href="/style.css" rel="stylesheet">
                    </head>
                    <form method="post" action="results" style="font-weight: bold; font-family: 'Arial', sans-serif;">
                      Enter a title: <input type="text" name="title" />
                      <button type="submit">Submit</button>
                    </form>
                <div>
                    
                """
    

    #second page to display the results page
    @cherrypy.expose
    def results(self, title):
        gathered_books = gather_similar_books(title)

        if gathered_books:
            # starts creating a long html code block with output
            output = "<h1>Recommended Books</h1><ul>"
            recommended_count = 0

            for book in gathered_books:
                # only want 5 recomendations
                if recommended_count >= 5:
                    break
                #unfortunately long execution time
                score = return_score(book)
                if score >= 4.0:
                    recommended_count += 1
                    output += f"<li>{book} - Rating: {score:.2f}</li>"

            output += "</ul>" # end of the html list
            return output
        else:
            return "<h1>No results found for this title.</h1>"

if __name__ == "__main__":
    conf = {
    '/': {
        'tools.sessions.on': True,
        'tools.staticdir.root': os.path.abspath(os.getcwd())
    },
    '/style.css': {
        'tools.staticfile.on': True,
        'tools.staticfile.filename': os.path.abspath("public/style.css"),
    }
}
    cherrypy.quickstart(DisplayResults())

driver.close()