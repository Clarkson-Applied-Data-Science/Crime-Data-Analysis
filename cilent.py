# %%
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



# API - 1 %%
print(totalcrimes(123,10011))

# API - 2%%
print(number_of_crimes(123,10011))

#API - 3 %%
print(crimespermonth(123,'08'))

# %%



