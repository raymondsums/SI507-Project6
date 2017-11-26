# Import statements
import psycopg2
import psycopg2.extras
import sys
from config import *
from csv import DictReader

# Write code / functions to set up database connection and cursor here.

def get_connection_and_cursor():
    try:
        if db_password != "":
            db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
            print("Success connecting to database")
        else:
            db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
    except:
        print("Unable to connect to the database. Check server and credentials.")
        sys.exit(1) # Stop running program if there's no db connection.

    db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

con, cur = get_connection_and_cursor()

cur.execute("DROP TABLE IF EXISTS Sites")
cur.execute("DROP TABLE IF EXISTS States")

cur.execute("CREATE TABLE IF NOT EXISTS States(ID SERIAL PRIMARY KEY, Name VARCHAR(40) UNIQUE)")
cur.execute("CREATE TABLE IF NOT EXISTS Sites(ID SERIAL, Name VARCHAR(128) UNIQUE, Type VARCHAR(128), State_ID INTEGER, Location VARCHAR(255), Description TEXT)")

file_arkansas = DictReader(open('arkansas.csv','r'))
file_michigan = DictReader(open('michigan.csv','r'))
file_california = DictReader(open('california.csv','r'))

for line_dict in file_arkansas:
    sql = """INSERT INTO Sites(Name,Type,State_ID,Location,Description) VALUES (%s,%s,%s,%s,%s)"""
    cur.execute(sql,(line_dict['NAME'],line_dict['TYPE'],1,(line_dict['ADDRESS']),line_dict['DESCRIPTION']))
for line_dict in file_michigan:
    sql = """INSERT INTO Sites(Name,Type,State_ID,Location,Description) VALUES (%s,%s,%s,%s,%s)"""
    cur.execute(sql,(line_dict['NAME'],line_dict['TYPE'],2,(line_dict['ADDRESS']),line_dict['DESCRIPTION']))
for line_dict in file_california:
    sql = """INSERT INTO Sites(Name,Type,State_ID,Location,Description) VALUES (%s,%s,%s,%s,%s)"""
    cur.execute(sql,(line_dict['NAME'],line_dict['TYPE'],3,(line_dict['ADDRESS']),line_dict['DESCRIPTION']))

cur.execute("INSERT INTO States(Name) VALUES ('AR')")
cur.execute("INSERT INTO States(Name) VALUES ('MI')")
cur.execute("INSERT INTO States(Name) VALUES ('CA')")
con.commit()

cur.execute("SELECT Location FROM Sites")
all_locations = []
for entry in cur.fetchall():
    all_locations.append(entry.values())

cur.execute("""SELECT Description FROM Sites WHERE Description ilike '%beautiful%'""")
beautiful_sites = cur.fetchall()

cur.execute("""SELECT count(*) FROM Sites WHERE Type ilike '%National Lakeshore%'""")
natl_lakeshores = cur.fetchall()

cur.execute("SELECT Sites.Name FROM Sites INNER JOIN States ON Sites.State_ID = States.ID WHERE States.ID = 2") #needs inner join method
michigan_name = cur.fetchall()

cur.execute("SELECT Count(*) FROM Sites INNER JOIN States ON Sites.State_ID = States.ID WHERE States.ID = 1")
total_number_arkansas = cur.fetchall()

# Write code / functions to create tables with the columns you want and all database setup here.

#setup tables with name, location, type, address and description as columns

# Write code / functions to deal with CSV files and insert data into the database here.

# Make sure to commit your database changes with .commit() on the database connection.

# Write code to be invoked here (e.g. invoking any functions you wrote above)

# Write code to make queries and save data in variables here.

# We have not provided any tests, but you could write your own in this file or another file, if you want.