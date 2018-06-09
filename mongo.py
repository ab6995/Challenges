from pymongo import MongoClient
import zmq
import json
import time
import re
import datetime
from datetime import timedelta


date = datetime.date.today()
date_convert = date.strftime("%m_%d_%y")

current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
end = datetime.datetime.combine(datetime.date.today(), datetime.time.max)+timedelta(hours=0)
last_time = end.strftime('%Y-%m-%d %H:%M:%S')


def contect_zmq(tcp_address = 't'):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(tcp_address)
    socket.setsockopt(zmq.SUBSCRIBE, "")
    return socket


def mongo_connection(ip="", database="", **kwargs):
    connection = MongoClient(ip, 27017)
    db = connection[database]
    db.authenticate(**kwargs)

    return db


def main():
    socket = contect_zmq()

    socket_187 = contect_zmq('')
    
    db = mongo_connection(ip="", database="", name='', password='', source='')
    db_cluster = mongo_connection(ip="", database="", name='', password='', source='')

    while (datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') < last_time):
        item = socket.recv()
        item_187 = socket_187.recv()

        if "ID" in item:
            timestamp = re.compile(r'"timestamp" : "(\d+)"')
            output_timestamp = timestamp.findall(item)[0]
            item = item.replace('"'+ output_timestamp + '"', output_timestamp)
            # print item
            itemjson = json.loads(item)
            today_timeSheet = date_convert
            collect = db["datamart_2_" + today_timeSheet]
            collect_2 = db_cluster["datamart_collection_" + today_timeSheet]
            
            collect.insert(itemjson)
            collect_2.insert(itemjson)

            index_name = 'ID'
            if index_name not in collect.index_information():
                collect.create_index(index_name)

            if index_name not in collect_2.index_information():
                collect_2.create_index(index_name)

        if "ID" in item_187:
            timestamp = re.compile(r'"timestamp" : "(\d+)"')
            output_timestamp = timestamp.findall(item_187)[0]
            item_187 = item_187.replace('"'+ output_timestamp + '"', output_timestamp)

            itemjson = json.loads(item_187)
            today_timeSheet = date_convert

            collect = db_cluster["187" + today_timeSheet]
            
            collect.insert(itemjson)

            index_name = 'ID'
            if index_name not in collect.index_information():
                collect.create_index(index_name)



if __name__ == "__main__":
    main()
