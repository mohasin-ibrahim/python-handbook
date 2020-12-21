# Requests Tutorial ..
import requests
import pandas as pd
import sqlite3 as sqlite
from sqlite3 import Error

req = requests
response = req.get("https://chroniclingamerica.loc.gov/search/titles/results/?terms=michigan&format=json&page=5")
#https://reqres.in/api/products/3


# Mostly Interested Metadata Information from the response object - Status Code, Encoding, URL, Headers
print("Status of the HTTP Call -- " + str(response.status_code))

# URL will be useful to validate while redirecting or checking the query string
#print(response.url)
#print(response.cookies)
#print(response.elapsed)

# Encoding can also be modified before reading the reponse content, for example
# response.encoding = 'ISO-8859-1'
# print(response.encoding)
# print(response.encoding)
#print(response.text)

# Text argument prints the content of the reponse as plain string
#print(response.text)
#print(response.history)

# From Headers Content-Type, Date, Content Type, Status will be useful to take further steps. Most of the time it works case insensitive like below
#print(response.headers)
print("Content-Type -- " + response.headers['coNtent-tYpE'])
print("Date -- " + response.headers['dATe'])

# Json method converts the content of the response to Json Object (dict - kV pairs) 
json_data = response.json()
#print(type(json_data))
#print(json_data)
# print(json_data['data']['id'])
# print(json_data["support"]["text"])
response.close()

count = 0
published_items= []
for element in json_data["items"]:
    count = count + 1
    published_items.append([element["id"], element["place_of_publication"], element["start_year"], element["publisher"], element["title"], element["end_year"] ])

df = pd.DataFrame(published_items, columns=['Id', 'PlaceOfPublication', 'StartYear', 'Publisher', 'Title', 'EndYear'])
# for item in published_items:
#     print(item[0])
print ("Total items Read from the response --- " + str(count))

def create_db_connection(filename):
    try:
        conn = sqlite.connect(filename)
        cursor = conn.cursor()
        cursor.execute('CREATE TABLE IF NOT EXISTS PUBLISHED_ITEMS (ID VARCHAR(25), PLACE_OF_PUBLISH VARCHAR(50), START_YEAR VARCHAR(5), PUBLISHER VARCHAR(50), TITLE VARCHAR(100), END_YEAR VARCHAR(5));')
        for i in range(15):
            cursor.execute("INSERT INTO PUBLISHED_ITEMS VALUES ('"+str(df.loc[i, 'Id'])+"','"+str(df.loc[i, 'PlaceOfPublication'])+"','"+str(df.loc[i, 'StartYear'])+"','"+str(df.loc[i, 'Publisher'])+"','"+str(df.loc[i, 'Title'])+"','"+str(df.loc[i, 'EndYear'])+"');")
        cursor.execute("SELECT * FROM PUBLISHED_ITEMS WHERE START_YEAR>'1850'")
        fetched_data = cursor.fetchall()
        for row in fetched_data:
            print(row)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()

# Update local directory is needed to work this code
create_db_connection(r"/home/blackpanther/python-automation/http-request-handling/src/db/sqlite3.db")



