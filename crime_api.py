# %%
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


# %%



