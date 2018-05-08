import os

#Starting kafka nodes
#Zookeeper
os.system("kafka_ZK/bin/zookeeper-server-start.sh kafka_ZK/config/zookeeper.properties &")

#DataNode0
os.system("kafka_N0/bin/kafka-server-start.sh kafka_N0/config/server.properties &")
#DataNode1
os.system("kafka_N1/bin/kafka-server-start.sh kafka_N1/config/server.properties &")
