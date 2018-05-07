#For Apache Kafka
from kafka import KafkaProducer
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

#For URL handling
from urllib2 import urlopen
import socket

#For encryption
# from Crypto.PublicKey import RSA
# from Crypto import Random


myIP = urlopen('http://ip.42.pl/raw').read()
myPrivateIP = socket.gethostbyname(socket.gethostname())
print("myPrivateIP", myPrivateIP)
producer = KafkaProducer(bootstrap_servers=[myIP+':9090'], api_version=(0,10))

#Assignment of arguments
topic = argv[1]

#Creation of message structure
msgJSON = {}

#Encrytion procedure
# random_generator = Random.new().read
# key = RSA.generate(1024, random_generator)
# print("encryption key: ", key)
# publicKey =  key.publickey()
# print("public key: ", publicKey)

def getBPM():
    data = {}
    data["data_type"] = dataType
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

def publish(msg):
    print("This message will be published: ", msg)
    print("In the topic: ", topic)
    producer.send(topic, msg)
    producer.flush()



#for publishing
def sendMsg(sc):
    print "Sending message..."
    ## TODO: what to publish
    toPublish = getBPM() #change This
    publish(toPublish)
    s.enter(5, 1, sendMsg, (sc,))

s = sched.scheduler(time.time, time.sleep)
s.enter(5, 1, sendMsg, (s,))
s.run()
