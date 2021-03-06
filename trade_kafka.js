var express = require("express");
var fs = require('fs');
var Kafka = require('node-rdkafka');
var net = require('net');

var app = express();
var bodyParser = require('body-parser');
var port = 3000;

app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

app.use(function(err, req, res, next){
  if(err.status && err.status < 500) {
        return res.status(404).send("<img src=\"https://d2v4zi8pl64nxt.cloudfront.net/the-most-entertaining-guide-to-landing-page-optimization-youll-ever-read/537a57c5c2de14.13737630.png\">");
      }

      console.log('Type of Error:', typeof err);
      console.log('Error: ', err.stack);

      if(req.xhr) {
        res.partial('500', { error: err });
      } else {
        res.render('500', { error: err });
      }
});

app.get("/",function(req,res){
 //TODO
 return res.status(404).send("<img src=\"https://d2v4zi8pl64nxt.cloudfront.net/the-most-entertaining-guide-to-landing-page-optimization-youll-ever-read/537a57c5c2de14.13737630.png\">");
});

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

app.listen(port,function(){
  console.log("Live at Port " + port);
});

///// Kafka ///////
//PRODUCER
app.post("/", function(req, res){
  console.log("HTTP POST request was received");
  var topic = req.body.topic; //public address
  //msgToSend may be taken from HTTP request
  console.log(req.body);
  var msgToSend = req.body.value;
  console.log("Topic to create", topic);
  console.log('Message to send', msgToSend);

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
app.get("/:topicName",function(req,res){

  const topic = [req.params.topicName];

  console.log("HTTP GET  request was received for topic", topic);

  //TODO: Define groups in the System for different companies.
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

  var offTimer = 10000;
  var extract = new Object;
  var values = [];
  consumer.on('data', function(data){
    console.log('Data Value', data.value);
    if (data.value != null) {
      offTimer += 1000;
      console.log('DATA', data);
      //TODO build json
      extract.topic = data.topic;
      extract.value = values;
      extract.value.push(data.value.toString());
      extract.timestamp = data.timestamp;
    }
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
    console.log(extract);
    console.log('Timeout('+ offTimer +') - Already up to date');
    res.status(200).send(extract);
    delete extract;
    consumer.disconnect();
  }, offTimer);
});
