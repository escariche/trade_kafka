#For Apache Kafka
from kafka import KafkaProducer
from kafka.cluster import ClusterMetadata
from kafka.errors import KafkaError

#For threading
import sched, time
import threading
from datetime import datetime
import random

#For json parsing
import json

#For argument reading
from sys import argv
import sys

#For URL handling
from urllib2 import urlopen
import socket

#For encryption
# from Crypto.PublicKey import RSA
# from Crypto import Random


myIP = urlopen('http://ip.42.pl/raw').read()
myPrivateIP = socket.gethostbyname(socket.gethostname())
print("myPrivateIP", myPrivateIP)
producer = KafkaProducer(bootstrap_servers=[myPrivateIP + ':9090', myPrivateIP + ':9091'], api_version=(0,10))
print(ClusterMetadata.brokers())
#Assignment of arguments
try:
    topic = argv[1]
    print("Topic found as arg ", topic)
except IndexError:
    print(IndexError)
    quit()

#Creation of message structure
msgJSON = {}

def getBPM():
    data = {}
    #data["data_type"] = dataType
    data["values"] = []

    sample = {}
    sample["timestamp"] = str(datetime.now())
    bpm = random.randint(60, 100)
    sample["value"] = bpm

    data["values"].append(sample)
    msgJSON["data"] = data

    json_formated_sample = json.dumps(msgJSON)
    print("Message as JSON: ", msgJSON)
    return str(json_formated_sample).encode()

# def publish(msg):
#     print("This message will be published: ", msg)
#     print("In the topic: ", topic)
#     producer.send(topic, msg)
#     producer.flush()
try:
    print("Sending routine")
    toPublish = getBPM()
    print("toPublish", toPublish)
    producer.send(topic, toPublish)
    producer.flush()
except:
    print("Error: ", sys.exc_info()[0])
    raise

producer.close(3600)

#for publishing
#def sendMsg():
    #print "Sending message..."
    ## TODO: what to publish
    #publish(toPublish)
    #s.enter(5, 1, sendMsg, (sc,))

#s = sched.scheduler(time.time, time.sleep)
#s.enter(5, 1, sendMsg, (s,))
#s.run()
