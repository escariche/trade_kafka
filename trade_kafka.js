var express = require("express");
// var PythonShell = require('python-shell');
var fs = require('fs');
var Kafka = require('node-rdkafka');

var app = express();
var router = express.Router();
var port = 3000;

// var viewsPath = __dirname + '/views/';
//TODO
var kafkaPath = '/home/ec2-user/trade_kafka/';
var dataPath = '/home/ec2-user/trade_kafka/consumer_data/';

router.use(function (req,res,next) {
  console.log("/" + req.method);
  next();
});

router.get("/",function(req,res){
  //TODO
  res.status(404).send("<img src=\"https://d2v4zi8pl64nxt.cloudfront.net/the-most-entertaining-guide-to-landing-page-optimization-youll-ever-read/537a57c5c2de14.13737630.png\">");
});

app.use("/",router);

app.listen(port,function(){
  console.log("Live at Port " + port);
});

// var options = {
//   mode: 'text',
//   scriptPath: '/home/ec2-user/trade_kafka/scripts',
//   pythonOptions: ['-u'],
//   args: []
// };


///// Kafka ///////
var brokerList;
//PRODUCER
router.post("/:topicName",function(req,res){
  console.log("HTTP POST request was received");
  var topic = req.params.topicName; //public address
  //msgToSend may be taken from HTTP request
  var msgToSend = Date.now().toString();
  console.log("Topic to create", topic);

  var producer = new Kafka.Producer({
    'metadata.broker.list' : '172.31.34.212:9090, 172.31.34.212:9091',
    'dr_cb' : true
  });

  producer.on('event.log', function(log) {
    console.log("LOG", log);
  });

  producer.on('event.error', function(err) {
    console.error('Error from producer');
    console.error(err);
  });
  producer.connect();

  producer.on('ready', function(){
    console.log("ready");
    producer.getMetadata(null, function(err, metadata){
      if (err) {
        console.error('Error getting metadata');
        console.error(err);
      } else {
        console.log('Got metadata');
        console.log(metadata);
      }
    });
    try {
        producer.produce(
          // Topic to send the message to
          topic,
          // optionally we can manually specify a partition for the message
          // this defaults to -1 - which will use librdkafka's default partitioner (consistent random for keyed messages, random for unkeyed messages)
          null,
          // Message to send. Must be a buffer
          new Buffer(msgToSend),
          // for keyed messages, we also specify the key - note that this field is optional
          null,
          // you can send a timestamp here. If your broker version supports it,
          // it will get added. Otherwise, we default to 0
          Date.now()
          // you can send an opaque token here, which gets passed along
          // to your delivery reports
        );
        res.status(200).send();
      } catch (err) {
        console.error('A problem occurred when sending our message');
        console.error(err);
        res.send(err);
      }
  });

  // Any errors we encounter, including connection errors
  producer.on('event.error', function(err) {
    console.error('Error from producer');
    console.error(err);
    res.send(err);
  });


  setTimeout(function() {
    producer.disconnect();
  }, 30000);
});

//CONSUMER
router.get("/stream/:topicName",function(req,res){
  console.log("HTTP GET stream request was received");
  var topic = req.params.topicName;
  //TODO EXTRA - create groups of subscribers
  var group = 'All Companies';
  console.log("Requested topic: " + topic);

  var stream = Kafka.KafkaConsumer.createReadStream({
    'metadata.broker.list': '172.31.34.212:9090, 172.31.34.212:9091',
    'group.id': group,
    'socket.keepalive.enable': true,
    'enable.auto.commit': false
  }, {}, {
    topics: topic,
    waitInterval: 0,
    objectMode: false
  });

  stream.on('error', function(err){
    if (err) {
      console.log(err);
    }
    res.send(err);
    //process.exit(1);
  });

  stream.on('data', function(message) {
    console.log('Got message');
    console.log(message.value.toString());
    res.status(200).send(message.value.toString());
  });
});

router.get("/consumer/:topicName",function(req,res){
  console.log("HTTP GET (consumer) request was received");
  const topic = [req.params.topicName];
  console.log("Requested topic: " + topic);
  const group = 'Standard Company';
  const kafkaConfig = {
    'group.id': group,
    "metadata.broker.list": '172.31.34.212:9090, 172.31.34.212:9091'
    //There are more detailed configurations
  };
  const consumer = new Kafka.KafkaConsumer(
    kafkaConfig,
  //   {
  //   'group.id': group,
  //   'metadata.broker.list': '172.31.34.212:9090, 172.31.34.212:9091',
  // },
  {
    "auto.offset.reset": "beginning"
  });
  const numMessages = 5;
  var counter = 0;

  consumer.on('error', function(err){
    console.log(err);
    res.send(err);
  });

  consumer.on('ready', function (arg){
    console.log('Consumer '+ arg.name+ ' ready');
    var metadataProm = new Promise(function(resolve, reject){
      consumer.getMetadata(null, function(err, metadata){
        if (err) {
          console.error('Error getting metadata');
          console.error(err);
          reject(err);
        } else {
          console.log('Got metadata');
          //console.log(metadata);
          resolve(metadata);
        }
      });
    });

    // consumer.subscribe(topic);
    // consumer.consume();

    metadataProm.then(function(metadata){
      console.log(' - MetadataProm - ');
      console.log(metadata);
      consumer.subscribe(topic);
      console.log('subs');
      consumer.consume();
      console.log('consume');
    }, function(reason){
      console.log(reason);
    });
  });

  var consumedData;
  consumer.on('data', function(data){
    counter ++;
    if(counter % numMessages === 0){
      console.log('Calling commit');
      consumer.commit(data);
    }
    console.log('Data found');
    consumedData += data.value.toString() + '\n';
    console.log(data.value.toString());
  });

  consumer.on('disconnected', function(arg){
    res.status(200).send(consumedData);
    consumedData = '';
    process.exit;
  });

  consumer.on('event.error', function(err){
    console.error(err);
    process.exit(1);
  });

  consumer.on('event.log', function(log) {
    console.log(log);
  });

  consumer.connect();

  setTimeout(function() {
    consumer.disconnect();
  }, 300000);
});
