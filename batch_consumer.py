from enum import unique
from urllib import response
from kafka import KafkaConsumer
from json import dumps, loads
from json import dump
import boto3

s3_client = boto3.resource('s3')

consumer = KafkaConsumer('PintrestTopic', bootstrap_servers='localhost:9092', value_deserializer=lambda x: loads(x), auto_offset_reset='earliest')


for i, msg in enumerate (consumer):
    msg = msg.value
    pintrest_obj = s3_client.Object('pinterest-data-34b08b92-1082-4d4f-a6e7-924a8ae2a66e',f'msg_{i}.json')
    pintrest_obj.put(Body=(bytes(dumps(msg).encode('UTF-8'))))
    #msg = dict(str(msg).replace("'", '"'))
    #print(msg)
    #file_name = msg.get("unique_id")
    #with open(file_name + '.json', "w") as write_file: dump(msg, write_file, indent=4)
    #response = s3_client.upload_file(file_name + '.json', 'pinterest-data-34b08b92-1082-4d4f-a6e7-924a8ae2a66e', file_name + '.json')
    print(msg)