#imports for kafka consumer
from kafka import KafkaConsumer, KafkaClient
from kafka.errors import KafkaError
#imports for local system
from sys import argv
import os.path
#imports for communication
from urllib2 import urlopen
import json

#KafkaConsumer settings
myIP = urlopen('http://ip.42.pl/raw').read()
consumer = KafkaConsumer(bootstrap_servers=[myIP+':9091'])

#TODO
consumerDataPath = '/home/ec2-user/kafka/kafka_2.11-1.0.0/consumer_data/'

topic = argv[1]
consumer.subscribe([topic])


if consumer.subscription() is not None:
    print(consumer.subscription())
else:
    print('No subscriptions')

#Message Handler
#Structure received -> ConsumerRecord(topic=u'test002', partition=0, offset=270, timestamp=1513069634605, timestamp_type=0, key=None, value='{"user_address": "0xC5fdf4076b8F3A5357c5E395ab970B5B54098Fef", "data": {"values": [{"timestamp": "2017-12-12 09:07:14.604932", "value": 97}], "data_type": "01"}, "company_address": "0x821aEa9a577a9b44299B9c15c88cf3087F3b5544"}', checksum=-436508159, serialized_key_size=-1, serialized_value_size=226)
print("LOOP FOR READING MESSAGES")
for message in consumer:
    print("RCVD MSG", message)
    print("VALUE FROM RCVD MSG", message.value)
    newMsgJSON = json.loads(message.value)
    print("RCVD MSG as JSON: ", newMsgJSON)
    oFilename = consumerDataPath + message.topic + '_val.json'
    if os.path.isfile(oFilename):
        print('File already exists')
        with open(oFilename, 'r') as json_file:
            storedTopicJSON = json.load(json_file)
            newValue = newMsgJSON["data"]["values"][0]
            print("New value to add: ", newValue)
            storedTopicJSON['data']['values'].append(newValue)
            print("Number of samples: ", len(storedTopicJSON['data']['values']))
            print("JSON after append: ", storedTopicJSON)
            with open(oFilename, "w") as outfile:
                json.dump(storedTopicJSON, outfile)
    else:
        print('File does not exist')
        content = newMsgJSON
        print("Content of new file: ", content)
        with open(oFilename, "w") as outfile:
            json.dump(content, outfile)
