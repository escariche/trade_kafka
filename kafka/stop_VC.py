import os

dir = "kafka_unified_settings/"
#Starting kafka nodes
#Zookeeper
os.system( dir + "bin/zookeeper-server-stopt.sh ")
#os.system("kafka_ZK/bin/zookeeper-server-start.sh kafka_ZK/config/zookeeper.properties &")

#DataNode0
#os.system(dir + "bin/kafka-server-start.sh " + dir +  "config/server_0.properties &")
#DataNode1
#os.system(dir + "bin/kafka-server-start.sh " + dir +  "config/server_1.properties &")
