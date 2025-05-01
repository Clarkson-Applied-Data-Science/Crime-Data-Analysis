<h1 align="center">  Crime Data Analysis via Flask API" </h1>

## **Project Overview:**
The Crime Data Analysis via Flask API project aims to provide insights into crime statistics based on various criteria such as location, crime type, and time period. It consists of a Flask application serving as the backend API for retrieving crime-related data, and a client script for making requests to the API. We developed three API'S,  
1. Take zipcode as argument and gives Total no.of case in the zipcode
2. Take zipcode as argument and gives count and type of offenses happend in the zipcose
3. .Take Month number as argument and gives top5 zipcodes of the most cases.

## **Data Description:**
The dataset contains arrest records with unique identifiers, arrest dates, offense descriptions, legal codes, borough, precinct, jurisdiction codes, demographic details, and geographical coordinates. It provides a comprehensive overview of criminal activities, facilitating analysis of crime trends and patterns. All null data entries were replaced with 0, while addresses with no corresponding information were labeled as "Not Found" for clarity and consistency within the dataset. This ensures completeness and uniformity, facilitating accurate analysis of crime statistics and trends.

## **Feature Description:**
| **Attribute**       | **Description**                                              |
|---------------------|--------------------------------------------------------------|
| **ARREST_KEY**      | Unique identifier for each arrest.                           |
| **ARREST_DATE**     | Date when the arrest occurred.                               |
| **PD_CD**           | New York Police Department code related to the offense.      |
| **PD_DESC**         | Description of the offense.                                  |
| **KY_CD**           | Another code related to the offense.                         |
| **OFNS_DESC**       | Description of the offense.                                  |
| **LAW_CODE**        | Legal code related to the offense.                           |
| **LAW_CAT_CD**      | Legal category of the offense.                               |
| **ARREST_BORO**     | The borough where the arrest took place.                     |
| **ARREST_PRECINCT** | The precinct where the arrest occurred.                      |
| **JURISDICTION_CODE** | Code indicating the jurisdiction.                          |
| **AGE_GROUP**       | The age group of the person arrested.                        |
| **PERP_SEX**        | Gender of the person arrested.                               |
| **PERP_RACE**       | Race of the person arrested.                                 |
| **Latitude**        | Latitude coordinates of the arrest location.                 |
| **Longitude**       | Longitude coordinates of the arrest location.                |

## **Inserting the Offense data**
``` python
import csv
import pymysql
from datetime import datetime

diction = {}
count = 0
csv_file = 'NYPD_Arrest_Data__Year_to_Date__20240403.csv' 
with open(csv_file,encoding='latin-1', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        count+=1
        if row['OFNS_DESC'] not in diction.keys():
            diction[row['OFNS_DESC']] = row['KY_CD']


conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='WRITE YOURS',
                       passwd='WRITE YOURS', db='WRITE YOURS_crime', autocommit=True)
cur = conn.cursor(pymysql.cursors.DictCursor)

# Dropping the table if it exists
sql = '''DROP TABLE IF EXISTS Offense_data'''
cur.execute(sql)

# Creating the Offense_data table
sql = """
CREATE TABLE Offense_data (
    `Offense_KEY` INT AUTO_INCREMENT PRIMARY KEY,
    `KY_CD` INT,
    `OFNS_DESC` VARCHAR(255)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
cur.execute(sql)

sql = '''
    INSERT INTO Offense_data (KY_CD, OFNS_DESC)
    VALUES (%s, %s)
    '''
start = datetime.now()
for key in diction.keys():
    if diction[key].isdigit()== True:
        tokens = (diction[key],key)
    else:
        print('data missing')
        tokens = (000,key)
    cur.execute(sql, tokens)
end = datetime.now()
print(f'Inserted Successfully in {end-start} seconds')
count
```

```
Output:
data missing
Inserted Successfully in 0:00:00.383604 seconds
```

**Explaination:** 
This Python script reads data from a CSV file containing NYPD arrest data, storing offense descriptions and their corresponding codes in a dictionary. It then establishes a connection to a MySQL database, drops any existing table named "Offense_data," creates a new one, and populates it with the offense data. Finally, it prints the time taken for the insertion process.

## **Inserting the Arrest data**
``` python
import pymysql
from datetime import datetime
import csv

count = 0
csv_file = 'NYPD_Arrest_Data__Year_to_Date__20240403.csv' 
with open(csv_file,encoding='latin-1', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    arrest_data = list(reader) 

conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='WRITE YOURS',
                       passwd='WRITE YOURS', db='WRITE YOURS_crime', autocommit=True)
cur = conn.cursor(pymysql.cursors.DictCursor)

# Dropping the table if it exists
sql = '''DROP TABLE IF EXISTS arrest_data'''
cur.execute(sql)

# Creating the arrest_data table
sql = """
CREATE TABLE arrest_data (
    ARREST_UNIQUE_ID INT AUTO_INCREMENT PRIMARY KEY,
    ARREST_KEY INT ,
    Offense_KEY INT,
    ARREST_DATE DATE,
    PD_CD INT,
    PD_DESC VARCHAR(255),
    LAW_CODE VARCHAR(50),
    LAW_CAT_CD VARCHAR(50),
    ARREST_BORO VARCHAR(50),
    ARREST_PRECINCT INT,
    JURISDICTION_CODE INT,
    AGE_GROUP VARCHAR(50),
    PERP_SEX VARCHAR(10),
    PERP_RACE VARCHAR(50),
    Latitude DECIMAL(10, 8),
    Longitude DECIMAL(11, 8)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
cur.execute(sql)

sql = '''
    INSERT INTO arrest_data (ARREST_KEY, Offense_KEY, ARREST_DATE, PD_CD, PD_DESC, LAW_CODE, LAW_CAT_CD, ARREST_BORO, ARREST_PRECINCT, JURISDICTION_CODE, AGE_GROUP, PERP_SEX, PERP_RACE, Latitude, Longitude)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
start = datetime.now()
for row in arrest_data:
    ofns_desc = row['OFNS_DESC'].replace("'", "''")  
    
    if row['PD_CD'].isdigit()==True:
        pdcd = row['PD_CD']
    else:
        pdcd = 000
    
    query = f"SELECT Offense_KEY FROM Offense_data WHERE OFNS_DESC LIKE '{ofns_desc}'"
    cur.execute(query)
    foreign = cur.fetchone()
    if foreign:
        tokens = (
            row["ÿARREST_KEY"],
            foreign['Offense_KEY'],
            datetime.strptime(row['ARREST_DATE'], '%m/%d/%y'), 
             
            pdcd,
            row['PD_DESC'],
            row['LAW_CODE'],
            row['LAW_CAT_CD'],
            row['ARREST_BORO'],
            row['ARREST_PRECINCT'],
            row['JURISDICTION_CODE'],
            row['AGE_GROUP'],
            row['PERP_SEX'],
            row['PERP_RACE'],
            row['Latitude'],
            row['Longitude'],
        )
        cur.execute(sql, tokens)

end = datetime.now()
print(f'Inserted Successfully in {end-start} seconds')
```

```
Inserted Successfully in 0:21:17.264357 seconds
```

**Explaination:** 
Above script processes NYPD arrest data from a CSV file, establishes a connection to a MySQL database, and creates a table named "arrest_data" to store the data. It then inserts parsed data from the CSV into this table after replacing single quotes in offense descriptions to avoid SQL syntax errors. Additionally, it maps offense descriptions to their respective keys obtained from a separate "Offense_data" table to ensure data integrity. Finally, it prints the time taken for the insertion process.

## **Accessing Geolocation Data with an API**
``` python
import csv
import pymysql
from datetime import datetime
import requests

def get_place_name(latitude, longitude):
    api_key = "AIzaSyCyswXEdKoK-24na0RADpzyjljNNQcidpk" 
    url = f"https://maps.googleapis.com/maps/api/geocode/json?latlng={latitude},{longitude}&key={api_key}"
    response = requests.get(url)
    data = response.json()
    if 'results' in data and data['results']:
        return data['results'][0]['formatted_address']
    else:
        return "Not found"
```

**Explaination:** 
Above script defines a function get_place_name that retrieves the name of a location based on its latitude and longitude coordinates using the Google Maps Geocoding API. It imports necessary modules (csv, pymysql, datetime, and requests). The function constructs a URL with the coordinates, sends a request to the API, parses the JSON response, and returns the formatted address if found, otherwise "Not found".

``` python
csv_file = 'NYPD_Arrest_Data__Year_to_Date__20240403.csv' 
with open(csv_file,encoding='latin-1', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    arrest_data = list(reader)


conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='WRITE YOURS',
                       passwd='WRITE YOURS', db='WRITE YOURS_crime', autocommit=True)
cur = conn.cursor(pymysql.cursors.DictCursor)



# Dropping the table if it exists
sql = '''DROP TABLE IF EXISTS address_api_data'''
cur.execute(sql)

# Creating the Offense_data table
sql = """
CREATE TABLE address_api_data (
    `Address_KEY` INT AUTO_INCREMENT PRIMARY KEY,
    `Arrest_ID` INT,
    `Address` VARCHAR(255),
    `Zipcode` INT
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
cur.execute(sql)

sql = '''
    INSERT INTO address_api_data (Arrest_ID,Address,Zipcode) VALUES (%s,%s, %s)
    '''
start = datetime.now()

c=0
for row in arrest_data:
    c+=1
    latitude = row['Latitude']
    longitude = row['Longitude']
    address = get_place_name(latitude, longitude)
    if address[-10:-5].isdigit():
        zip_ = int(address[-10:-5])
    else:
        zip_ = 0
        
    tokens = (c,address,zip_)
    cur.execute(sql, tokens)
end = datetime.now()
print(f'Successfully inserted {c} observations in {end-start} seconds')
```

**Explaination:** 
Above code reads NYPD arrest data from a CSV file and inserts it into a MySQL database after retrieving the corresponding address and zipcode using the Google Maps Geocoding API. It establishes a connection to the database, creates a table for the address data, and inserts each arrest record along with its address and zipcode into the table. Finally, it prints the number of observations successfully inserted along with the time taken for the operation.


``` python
import pymysql
from datetime import datetime
import csv

count = 0
csv_file = 'NYPD_Arrest_Data__Year_to_Date__20240403.csv' 
with open(csv_file,encoding='latin-1', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    arrest_data = list(reader) 

conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='WRITE YOURS',
                       passwd='WRITE YOURS', db='WRITE YOURS_crime', autocommit=True)
cur = conn.cursor(pymysql.cursors.DictCursor)

# Dropping the table if it exists
sql = '''DROP TABLE IF EXISTS arrest_data'''
cur.execute(sql)

# Creating the arrest_data table
sql = """
CREATE TABLE arrest_data (
    ARREST_UNIQUE_ID INT AUTO_INCREMENT PRIMARY KEY,
    ARREST_KEY INT ,
    Offense_KEY INT,
    ARREST_DATE DATE,
    PD_CD INT,
    PD_DESC VARCHAR(255),
    LAW_CODE VARCHAR(50),
    LAW_CAT_CD VARCHAR(50),
    ARREST_BORO VARCHAR(50),
    ARREST_PRECINCT INT,
    JURISDICTION_CODE INT,
    AGE_GROUP VARCHAR(50),
    PERP_SEX VARCHAR(10),
    PERP_RACE VARCHAR(50),
    Latitude DECIMAL(10, 8),
    Longitude DECIMAL(11, 8)
) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
"""
cur.execute(sql)

sql = '''
    INSERT INTO arrest_data (ARREST_KEY, Offense_KEY, ARREST_DATE, PD_CD, PD_DESC, LAW_CODE, LAW_CAT_CD, ARREST_BORO, ARREST_PRECINCT, JURISDICTION_CODE, AGE_GROUP, PERP_SEX, PERP_RACE, Latitude, Longitude)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    '''
start = datetime.now()
for row in arrest_data:
    ofns_desc = row['OFNS_DESC'].replace("'", "''")  
    
    if row['PD_CD'].isdigit()==True:
        pdcd = row['PD_CD']
    else:
        pdcd = 000
    
    query = f"SELECT Offense_KEY FROM Offense_data WHERE OFNS_DESC LIKE '{ofns_desc}'"
    cur.execute(query)
    foreign = cur.fetchone()
    if foreign:
        tokens = (
            row["ÿARREST_KEY"],
            foreign['Offense_KEY'],
            datetime.strptime(row['ARREST_DATE'], '%m/%d/%y'), 
             
            pdcd,
            row['PD_DESC'],
            row['LAW_CODE'],
            row['LAW_CAT_CD'],
            row['ARREST_BORO'],
            row['ARREST_PRECINCT'],
            row['JURISDICTION_CODE'],
            row['AGE_GROUP'],
            row['PERP_SEX'],
            row['PERP_RACE'],
            row['Latitude'],
            row['Longitude'],
        )
        cur.execute(sql, tokens)

end = datetime.now()
print(f'Inserted Successfully in {end-start} seconds')
```

**Explaination:** 
Above code imports CSV data into MySQL, sanitizing inputs to prevent SQL injection, creating a table, and inserting rows. It handles non-numeric values and uses parameterized queries for security. The process includes querying existing records for foreign keys and reporting successful insertion time. Though generally efficient, it could optimize for larger datasets and improve error handling for robustness.

## **Accessing Crime Data for Flask API:**

``` python
import json,requests,time,pymysql
from flask import Flask,request,redirect

app = Flask(__name__)

#http://127.0.0.1:5000/crimecountdata?key=123&zipcode=11103
@app.route('/crimecountdata', methods=['GET'])
def get_crimecount_data():
    
    results = {}
    
    results['request'] = '/crimecountdata'
    
    key_=int(request.args.get('key'))
    if key_ is None or key_ != 123:
        results['message'] = 'Key is not valid'
        return json.dumps(results,indent=4)
    
    zip_ = request.args.get('zipcode')
    if zip_.isdigit():
        zipcode = int(zip_)
    else:
        results['message'] = 'Zipcode is not valid'
        return json.dumps(results,indent=4)
        
    conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='veerapsh',
                       passwd='Sri934774@', db='veerapsh_NY_crime', autocommit=True)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    
    query = f'''SELECT COUNT(Address_KEY) as Total_Crimes FROM address_api_data where Zipcode = {zipcode};'''
    s_time=time.time()
    cur.execute(query)
    e_time=time.time()
    working_time=e_time-s_time
    results['time_taken']=working_time
    items=[]
    for row in cur:
        items.append(row)
    if len(items)== 0:
        results['Result']='There is no crime data in this zipcode area'
        return json.dumps(results,indent=4,sort_keys=True, default=str)
    else:
        results['Result']=items
        return json.dumps(results,indent=4,sort_keys=True, default=str)




#http://127.0.0.1:5000/crimedata?key=123&zipcode=11103
@app.route('/crimedata', methods=['GET'])
def get_crime_data():
    
    results = {}
    
    results['request'] = '/crimedata'
    
    key_=int(request.args.get('key'))
    if key_ is None or key_ != 123:
        results['message'] = 'Key is not valid'
        return json.dumps(results,indent=4)
    
    zip_ = request.args.get('zipcode')
    if zip_.isdigit():
        zipcode = int(zip_)
    else:
        results['message'] = 'Zipcode is not valid'
        return json.dumps(results,indent=4)
        
    conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='veerapsh',
                       passwd='Sri934774@', db='veerapsh_NY_crime', autocommit=True)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    
    query = f'''SELECT o.OFNS_DESC, COUNT(o.OFNS_DESC) as Number_of_crimes
               FROM Offense_data o, arrest_data ar, address_api_data adp
               WHERE adp.Zipcode = {zipcode}
               AND o.Offense_KEY = ar.Offense_KEY
               AND ar.ARREST_UNIQUE_ID = adp.Arrest_ID
               GROUP BY o.OFNS_DESC
               ORDER by COUNT(o.OFNS_DESC)'''
    s_time=time.time()
    cur.execute(query)
    e_time=time.time()
    working_time=e_time-s_time
    results['time_taken']=working_time
    items=[]
    for row in cur:
        items.append(row)
    if len(items)== 0:
        results['Result']='There is no crime data in this zipcode area'
        return json.dumps(results,indent=4,sort_keys=True, default=str)
    else:
        results['Result']=items
        return json.dumps(results,indent=4,sort_keys=True, default=str)
    
    

#http://127.0.0.1:5000/monthcrimesdata?key=123&month=01
@app.route('/monthcrimesdata', methods=['GET'])
def get_countofcrime_data():
    
    results = {}
    
    results['request'] = '/monthcrimesdata'
    
    key_=int(request.args.get('key'))
    if key_ is None or key_ != 123:
        results['message'] = 'Key is not valid'
        return json.dumps(results,indent=4)
    
    s_date = request.args.get('month')
    if s_date is None:
        results['code'] = 2
        results['message']='Enter the month number' 
        return json.dumps(results, indent=4)
    conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='veerapsh',
                       passwd='Sri934774@', db='veerapsh_NY_crime', autocommit=True)
    cur = conn.cursor(pymysql.cursors.DictCursor)
    
    query = f'''SELECT adp.Zipcode, count(adp.Zipcode) as Number_of_crimes
                FROM Offense_data o, arrest_data ar, address_api_data adp
                WHERE ar.ARREST_DATE LIKE '%-{s_date}-%'
                AND o.Offense_KEY = ar.Offense_KEY
                AND ar.ARREST_UNIQUE_ID = adp.Arrest_ID
                GROUP BY adp.Zipcode
                ORDER by count(adp.Zipcode) DESC
                LIMIT 5;'''
    
    s_time=time.time()
    cur.execute(query)
    e_time=time.time()
    working_time=e_time-s_time
    results['time_taken']=working_time
    items=[]
    for row in cur:
        items.append(row)
    if len(items)== 0:
        results['Result']='There is no crime data in between these dates'
        return json.dumps(results,indent=4,sort_keys=True, default=str)
    else:
        results['Result']=items
        return json.dumps(results,indent=4,sort_keys=True, default=str)
    

if __name__=='__main__':
    app.run()
```

```
Output :
 * Serving Flask app "__main__" (lazy loading)
 * Environment: production
   WARNING: This is a development server. Do not use it in a production deployment.
   Use a production WSGI server instead.
 * Debug mode: off
 * Running on http://127.0.0.1:5000/ (Press CTRL+C to quit)
127.0.0.1 - - [26/Apr/2024 17:55:04] "[37mGET /crimecountdata?key=123&zipcode=11103 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 17:55:04] "[37mGET /crimedata?key=123&zipcode=11103 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 17:55:04] "[37mGET /monthcrimesdata?key=123&month=1 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 17:56:13] "[37mGET /crimecountdata?key=123&zipcode=11103 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 17:56:13] "[37mGET /crimedata?key=123&zipcode=11103 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 17:56:13] "[37mGET /monthcrimesdata?key=123&month=01 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 18:05:08] "[37mGET /crimecountdata?key=123&zipcode=11103 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 18:05:08] "[37mGET /crimedata?key=123&zipcode=11103 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 18:05:29] "[37mGET /crimedata?key=123&zipcode=11103 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 18:06:24] "[37mGET /monthcrimesdata?key=123&month=01 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 18:06:39] "[37mGET /crimecountdata?key=123&zipcode=11103 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 18:06:40] "[37mGET /crimedata?key=123&zipcode=11103 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 18:06:40] "[37mGET /monthcrimesdata?key=123&month=01 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 18:07:36] "[37mGET /crimecountdata?key=123&zipcode=11103 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 18:07:41] "[37mGET /crimedata?key=123&zipcode=11103 HTTP/1.1[0m" 200 -
127.0.0.1 - - [26/Apr/2024 18:08:12] "[37mGET /monthcrimesdata?key=123&month=01 HTTP/1.1[0m" 200 -
```

**Explaination:** 
This Flask API retrieves crime data from a MySQL database. It offers endpoints to fetch total crimes and crime types by ZIP code, and top 5 ZIP codes with the highest crime count for a specified month. It requires a valid API key and provides execution time for queries.

## **Interacting with a Crime Data API**
``` python
import json,requests,datetime,pymysql
from flask import Flask,redirect,request

def totalcrimes(key,zipcod):
    url = f'http://127.0.0.1:5000/crimecountdata?key={key}&zipcode={zipcod}'
    d=requests.get(url)
    result=json.loads(d.text)
    return result


def number_of_crimes(key,zipcod):
    url = f'http://127.0.0.1:5000/crimedata?key={key}&zipcode={zipcod}'
    d=requests.get(url)
    result=json.loads(d.text)
    return result


def  crimespermonth(key,month_number):
    url = f'http://127.0.0.1:5000/monthcrimesdata?key={key}&month={month_number}'
    d=requests.get(url)
    result=json.loads(d.text)
    return result

```

**Explaination:** 
These functions interact with a Flask API to fetch crime data. `totalcrimes()` retrieves total crimes by ZIP code, `number_of_crimes()` fetches crime types by ZIP code, and `crimespermonth()` gets the top 5 ZIP codes with the highest crime count for a given month. 

``` python
# API - 1 %%
print(totalcrimes(123,11205))

# API - 2%%
print(number_of_crimes(123,11205))

#API - 3 %%
print(crimespermonth(123,'02'))
```

Output:
![alt text](<image-1.png>)

## **Validations :**
**Input - 1:**
``` python
# API - 1 %%
print(totalcrimes(123,10462))

# API - 2%%
print(number_of_crimes(123,10462))

#API - 3 %%
print(crimespermonth(123,'06'))
```

**Output - 1:**
![alt text](image-2.png)

**Input - 2:**
``` python
# API - 1 %%
print(totalcrimes(123,10011))

# API - 2%%
print(number_of_crimes(123,10011))

#API - 3 %%
print(crimespermonth(123,'08'))
```

**Output - 2:**
![alt text](image-3.png)

**Input - 3:**
``` python
API - 1 %%
print(totalcrimes(123,11428))

API - 2%%
print(number_of_crimes(123,11428))

API - 3 %%
print(crimespermonth(123,'12'))
```

**Output - 3:**
![alt text](image-4.png)

## **Conclusion:**
The Crime Data Analysis via Flask API project offers a comprehensive solution for accessing and analyzing crime statistics. Leveraging Flask and MySQL, it provides three key APIs for retrieving crime data based on location, crime type, and time period. Through meticulous implementation and clear documentation, the project enables efficient data insertion, geolocation data retrieval, and seamless interaction with the Flask API. With validation examples demonstrating robustness, the project serves as a valuable tool for researchers, law enforcement agencies, and policymakers, empowering them to make informed decisions and take proactive measures to address crime-related challenges effectively.
