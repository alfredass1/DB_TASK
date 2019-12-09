import sqlite3
from movies.movie import movie
from movies.profit import profit


def open_connection():
    connection = sqlite3.connect("movies.db")
    cursor = connection.cursor()
    return connection, cursor


def close_connection(connection, cursor):
    cursor.close()
    connection.close()


def create_movies_table():
    try:
        connection, cursor = open_connection()
        query = """CREATE TABLE IF NOT EXISTS movies (
                    movie_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    movie_title TEXT UNIQUE,
                    director TEXT,
                    movie_year TEXT,                
                    movie_minutes REAL)
                """

        cursor.execute(query)

    except sqlite3.DatabaseError as error:
        print(error)

    finally:
        close_connection(connection, cursor)


def create_profits_table():
    try:
        connection, cursor = open_connection()
        query = """CREATE TABLE IF NOT EXISTS profits(
                    report_id integer PRIMARY KEY AUTOINCREMENT,
                    movie_rating TEXT ,
                    international_sales TEXT
                )
                """

        cursor.execute(query)

    except sqlite3.DatabaseError as error:
        print(error)

    finally:
        close_connection(connection, cursor)


create_movies_table()
create_profits_table()


def query_database(query, params=None):
    try:
        connection, cursor = open_connection()
        if params:
            cursor.execute(query, params)
            connection.commit()
        else:
            for row in cursor.execute(query):
                print(row)

    except sqlite3.DataError as error:
        print(error)

    finally:
        close_connection(connection,cursor)


def create_movie(movie):
    query = "INSERT INTO movies VALUES (? ,?, ?, ?, ?)"
    params = (movie.movie_id, movie.movie_title, movie.director, movie.movie_year, movie.movie_minutes)
    query_database(query, params)


movie1 = movie(None, "Toy Story", "John Lasseter", 1995, 81)
movie2 = movie(None, "Toy Story2", "John Lasseter", 1998, 81)


# create_movie(movie1)


def get_movie(movie):
    query = "SELECT * FROM movies WHERE movie_id = (?) OR movie_title = (?) OR director = (?) OR  movie_year =" \
            " (?) OR movie_minutes = (?)"
    params = (movie.movie_id, movie.movie_title, movie.director, movie.movie_year, movie.movie_minutes)
    query_database(query, params)


get_movie(movie1)


def update_movie(movie):
    query = "UPDATE movies SET movie_title = 'Bet kas' WHERE movie_title = (?)"
    params = (movie.movie_title,)
    query_database(query, params)


# update_movie(movie1)


def delete_movie(movie):
    query = "DELETE FROM movies WHERE movie_title = (?) OR movie_id = (?) OR director = (?) OR movie_year = (?)"
    params = (movie.movie_title, movie.movie_id, movie.director, movie.director)
    query_database(query, params)


# delete_movie(movie2)

def create_profit(profit):
    query = "INSERT INTO profits VALUES (? ,?, ?)"
    params = (profit.report_id, profit.movie_rating, profit.international_sales)
    query_database(query, params)


profit1 = profit(None, 8.2, 19950)


# profit2 = profit(None, 8.3, 19980)


# create_profit(profit1)


def get_profit(profit):
    query = "SELECT * FROM profits WHERE report_id = (?) OR movie_rating = (?) OR international_sales  = (?)"
    params = (profit.report_id, profit.movie_rating, profit.international_sales)
    query_database(query, params)


get_profit(profit1)


def update_profit(profit):
    query = "UPDATE profits SET movie_rating = '10' WHERE report_id = (?)"
    params = (profit.movie_rating,)
    query_database(query, params)


# update_profit(profit1)


def delete_profit(profit):
    query = "DELETE FROM profits WHERE report_id = (?) OR movie_rating = (?) OR international_sales = (?)"
    params = (profit.report_id, profit.movie_rating, profit.international_sales)
    query_database(query, params)


# delete_profit(profit2)

# Relations metodas one to one
def create_table_junction():
    try:
        connection = sqlite3.connect("movies.db")
        connection_cursor = connection.cursor()
        connection_cursor.execute("""CREATE TABLE IF NOT EXISTS junction (
                                                    first_id int,
                                                    second_id int,
                                                    FOREIGN KEY (first_id) REFERENCES movies(movie_id),
                                                    FOREIGN KEY (second_id) REFERENCES profits(report_id)
                                                    )""")
        connection.commit()

    except sqlite3.DataError as error:
        print(error)

    finally:
        connection.close()


movie2 = movie(None, "Toy Story", "John Lasseter", 1995, 81)
profit2 = profit(None, 8.2, 19950)
profit3 = profit(1, 8.2, 19950)

create_table_junction()


def insert_junction(movie, report):
    insert_table_junction_query = """INSERT INTO junction (first_id, second_id)
                                        SELECT(SELECT movie_id FROM movies WHERE movie_title=(?)), 
                                        (SELECT report_id FROM profits WHERE report_id=?)"""
    param = (movie, report)
    query_database(insert_table_junction_query, param)


insert_junction(movie1.movie_title, profit3.report_id)


def get_junction():
    query = """SELECT * FROM junction JOIN movies ON junction.first_id = movies.movie_id 
    JOIN profits ON junction.second_id = profits.report_id"""

    query_database(query)


get_junction()
