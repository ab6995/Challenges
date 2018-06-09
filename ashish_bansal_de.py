
# coding: utf-8


# importing the modules
import requests
import mysql.connector
from mysql.connector import errorcode
import json
import time
import pandas as pd

t1 = time.time()


### Connecting with MySQL Database

try:
    config = {
        'user':'cogo_read_only', 
        'password':'N&f#vSq9',
        'host':'data-engineer-rds.czmkgxqloose.us-east-1.rds.amazonaws.com', 
        'port' : '3306'
    }
    cnx_liveworks = mysql.connector.connect(**config)
    cursor = cnx_liveworks.cursor()
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("Database does not exist")
    else:
        print(err)


### Counting the Records in SQL Table


def count_sql_records(cursor):
    try:
        sql_sel_v1 = "SELECT count(DISTINCT emd5) FROM liveworks.cogo_list_v1;"
        # Execute the SQL query and get the response
        cursor.execute(sql_sel_v1)
        response = cursor.fetchall()
        liveworks_records = response[0]
        return liveworks_records[0]
    except:
        print("The count query Failed")


### Getting the stats for API


def api_stats(cursor):
    try:
        # api-endpoint
        URL = "http://EC2Co-EcsEl-MT18UEPNPS93-1308701016.us-east-1.elb.amazonaws.com:8000/records?page=1"
        # sending get request and saving the response as response object
        r = requests.get(url = URL)
        rj = json.loads(r.text)
        #finding the total number of pages
        num_pages =  rj['num_pages']
        #finding the total number of records
        cogo_records = rj['num_rows']
        live_cogo = pd.DataFrame(columns = ['emd5', 'live_job', 'live_company', 'company', 'job'])
        return cogo_records, num_pages
    except:
        print("The page does not have desired data")


### Comparing the values


def get_common_records(cursor, num_pages):
    page = 0
    live_cogo = pd.DataFrame(columns = ['emd5', 'live_job', 'live_company', 'company', 'job'])
    while(page < num_pages):
        page += 1
        URL = "http://EC2Co-EcsEl-MT18UEPNPS93-1308701016.us-east-1.elb.amazonaws.com:8000/records?page=" + str(page)
        r = requests.get(url = URL)
        rj = json.loads(r.text)
        cogo_df = pd.DataFrame(rj['rows'][0:]).drop(['address', 'birthdate', 'id', 'name', 'sex'], axis = 1)
        row_5 = rj['rows']
        myvalues = tuple(i['emd5'] for i in row_5 if 'emd5' in i)
        format_strings = ','.join(['%s'] * len(myvalues))
        try:
            cursor.execute("SELECT emd5,job,company FROM liveworks.cogo_list_v1 WHERE emd5 IN (%s);" % format_strings,myvalues)
            try:
                response = cursor.fetchall()
            except:
                print("The database did not have any matching records")
        except:
            print("The query syntax is incorrect")
        live_works= pd.DataFrame(response, columns = ['emd5','live_job','live_company'])
        #left join live_works df with cogo_df on emd5 (finally you'll have all the intersections)
        live_cogo_page = pd.merge(live_works, cogo_df, how='left', on= 'emd5')
        #append these records to main dataframe
        live_cogo = pd.concat([live_cogo, live_cogo_page])
    live_cogo.set_index('emd5', inplace= True)
    live_cogo = live_cogo[~live_cogo.index.duplicated(keep='first')]
    cursor.close()
    cnx_liveworks.close()
    return live_cogo


def myformat(x):
    return ('%.2f' % x).rstrip('0').rstrip('.')



def percentage(live_cogo):
    per = len(live_cogo[live_cogo['job'] == live_cogo['live_job']])
    per *= 100
    try:
        per /= len(live_cogo)
        per = 100 - per
        return myformat(per)
    except:
        print("The dataset does not have any values")



def save_to_csv(live_cogo):
    live_cogo['liveworks'] = '{"'+live_cogo['live_job']+'" : "'+live_cogo['live_company']+'"}'
    live_cogo['cogo'] = '{"'+live_cogo['job']+'" : "'+live_cogo['company']+'"}'
    live_cogo.drop(['live_job', 'live_company', 'company', 'job'], axis=1, inplace=True)
    live_cogo.to_csv('live_cogo.csv', header = False)


def save_output(live_cogo, liveworks_records, cogo_records, per ):
    common_records = len(live_cogo)
    t2 = time.time() - t1
    create_database = "CREATE TABLE \n `Liveworks`.`liveworks_cogo_list` (\`emd5` INT NOT NULL,\n    `cogo` JSON NULL,\n    `liveworks` JSON NULL,\n    PRIMARY KEY (`emd5`));"
    with open('results.txt', 'w') as f:
        f.write("The total time taken by the script to run output : {} secs. \n".format( str(t2)))
        f.write("The number of users within both the dataset(common) : {}.\n".format(common_records))
        f.write("The number of users only within cogo Dataset : {}.\n".format(liveworks_records - common_records))
        f.write("The number of users only within cogo Dataset : {}.\n".format(cogo_records - common_records))
        f.write("Percentage of Users with different Job Title :{}%.\n".format(per))       
    live_cogo[:10].to_csv('results.txt', mode='a')
    with open('results.txt', 'a') as f:
        f.write("\nThe statement for creating database is: {}.\n".format(create_database))
        f.close()
        


def main():
    liveworks_records = count_sql_records(cursor)
    cogo_records, num_pages = api_stats(cursor)
    live_cogo = get_common_records(cursor, num_pages)
    per = percentage(live_cogo)
    save_to_csv(live_cogo)
    save_output(live_cogo, liveworks_records, cogo_records, per)



main()

