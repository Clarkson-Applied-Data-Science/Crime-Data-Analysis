# %% [markdown]
# ### Inserting Offense data

# %%
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


conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='kotarih',
                       passwd='Newyork@13', db='kotarih_crime', autocommit=True)
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

# %% [markdown]
# ### Inserting Arrest data

# %%
import pymysql
from datetime import datetime
import csv

count = 0
csv_file = 'NYPD_Arrest_Data__Year_to_Date__20240403.csv' 
with open(csv_file,encoding='latin-1', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    arrest_data = list(reader) 

conn = pymysql.connect(host='mysql.clarksonmsda.org', port=3306, user='kotarih',
                       passwd='Newyork@13', db='kotarih_crime', autocommit=True)
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

# %%



