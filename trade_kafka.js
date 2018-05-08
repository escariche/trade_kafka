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
  res.send("<img src=\"https://d2v4zi8pl64nxt.cloudfront.net/the-most-entertaining-guide-to-landing-page-optimization-youll-ever-read/537a57c5c2de14.13737630.png\">");
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

//PRODUCER
router.post("/:topicName",function(req,res){
  console.log("HTTP POST request was received");
  var topic = req.params.topicName; //public address
  console.log(topic);

  var producer = new Kafka.Producer({
    'metadata.broker.list' : 'localhost:9090, localhost:9091'
  });

  var stream = Kafka.Producer.createWriteStream({
    'metadata.broker.list' : 'localhost:9090, localhost:9091'
  }, {}, {
    topic : topic
  });

  var queuedSuccess = stream.write(new Buffer('Example msg'));

  if (queuedSuccess) {
    console.log('We queued our message!');
  } else {
  // Note that this only tells us if the stream's queue is full,
  // it does NOT tell us if the message got to Kafka!  See below...
  console.log('Too many messages in our queue already');
  }

  stream.on('error', function (err) {
  // Here's where we'll know if something went wrong sending to Kafka
  console.error('Error in our kafka stream');
  console.error(err);
})

  // options.args.push(topic);
  // console.log("options.args", options.args);
  //console.log(req.body);
  // PythonShell.run('producer.py', options, function(err, results) {
  //   options.args = []
  //   console.log("attempt to empty options.args", options.args);
  //   if (err) {
  //     console.log("Error when running producer.py", err);
  //     res.send(err);
  //     return;
  //   }
  //   res.sendStatus(200);
  // });
});

//CONSUMER - HISTORIC
router.get("/historic/:topicName",function(req,res){
  console.log("HTTP GET/historic request was received");
  var topic = req.params.topicName;
  console.log("Requested topic: " + topic);
  requestedTopicPath = dataPath + topic + '_val.json';
  fs.stat(requestedTopicPath, function(err, data) {
    if (err.code == 'ENOENT') {
      console.log('The historical from the requested topic was not found.', err);
      res.send(err);
      return;
    } else if (err){
      console.log(err);
      res.send(err);
      return;
    }
    console.log('Requested topic exists');
    res.sendFile(requestedTopicPath);
  });
});

//CONSUMER - SUBSCRIBE
router.get("/subscribe/:topicName",function(req,res){
  console.log("HTTP GET/subscribe request was received");
  var topic = req.params.topicName;
  console.log("Requested topic: " + topic);
  options.args.push(topic);
  console.log("options.args", options.args);
  /*requestedTopicPath = dataPath + topic + '_val.json';
  fs.stat(requestedTopicPath, function(err, data) {
    if (err.code == 'ENOENT') {
      console.log('The historical from the requested topic was not found.', err);
      res.send(err);
      return;
    } else if (err){
      console.log(err);
      res.send(err);
      return;
    }
    console.log('Requested topic exists');
  });*/
  PythonShell.run('consumer.py', options, function (err, results) {
    options.args = []
    console.log("attempt to empty options.args", options.args);
    if (err) {
      console.log('Error when running consumer.py script: ' + err);
      res.send(err);
      return;
    }
    console.log('Running consumer.py script for subscribing to requested topic.');
    console.log('results: %j', results);
  });
  });
