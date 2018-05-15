var express = require("express");
var fs = require('fs');
var Kafka = require('node-rdkafka');
var net = require('net');

var app = express();
var bodyParser = require('body-parser');
var router = express.Router();
var port = 3000;

router.use(function (req,res,next) {
  console.log("/" + req.method);
  next();
});

router.get("/",function(req,res){
  //TODO
  res.status(404).send("<img src=\"https://d2v4zi8pl64nxt.cloudfront.net/the-most-entertaining-guide-to-landing-page-optimization-youll-ever-read/537a57c5c2de14.13737630.png\">");
});

app.use("/",router);
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.listen(port,function(){
  console.log("Live at Port " + port);
});

///// Kafka ///////
var brokerList;
//PRODUCER
// router.post("/:topicName",function(req,res){
app.post("/:topicName", function(req, res){
  console.log("HTTP POST request was received");
  var topic = req.params.topicName; //public address
  //msgToSend may be taken from HTTP request
  var msgToSend = req.body;
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

router.get("/:topicName",function(req,res){
  const topic = [req.params.topicName];
  console.log("HTTP GET  request was received for topic", topic);
  const group = 'Standard Company';
  const kafkaConfig = {
    'group.id': group,
    "metadata.broker.list": '172.31.34.212:9090, 172.31.34.212:9091'
    //There are more detailed configurations
  };
  const consumer = new Kafka.KafkaConsumer(
    kafkaConfig,{
    "auto.offset.reset": "smallest",
    "auto.commit.enable": "true"
  });


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

    metadataProm.then(function(metadata){
      consumer.unsubscribe();
      console.log('unsubs');
      consumer.subscribe(topic);
      console.log('subs');
      consumer.consume();
      console.log('consume');
    }, function(reason){
      console.log(reason);
    });
  });

  var consumedData = '';
  var offTimer = 10000;
  consumer.on('data', function(data){
    offTimer += 1000;
    console.log('DATA', data);
    console.log('offset', data.offset);
    consumedData += data.value.toString() + '\n';
    console.log(data.value.toString());
  });

  consumer.on('disconnected', function(arg){
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
    console.log('Timeout('+ offTimer +') - Already up to date');
    consumedData += '\n Up to date \n';
    res.status(200).send(consumedData);
    consumedData = '';
    consumer.disconnect();
  }, offTimer);
});
