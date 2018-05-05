var express = require("express");
var PythonShell = require('python-shell');
var fs = require('fs');

var app = express();
var router = express.Router();
var port = 80;

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

var options = {
  mode: 'text',
  scriptPath: '/home/ec2-user/trade_kafka/scripts',
  pythonOptions: ['-u'],
  args: []
};

//PRODUCER
router.post("/:topicName",function(req,res){
  console.log("HTTP POST request was received");
  var topic = req.params.topicName; //public address
  options.args.push(topic);
  console.log(req.body);
  console.log(topic);
  PythonShell.run('producer.py', options, function (err, results){
      if (err) throw err;
      console.log('results: %j', results);
    });
    res.sendStatus(200);
  });

//CONSUMER
router.get("/:topicName",function(req,res){
    var topic = req.params.topicName;
    options.args.push(topic);
    console.log("Requested topic: " + topic);
    //TODO
    requestedTopicPath = dataPath + topic + '_val.json'
    fs.stat(requestedTopicPath, function(err, data) {
      if (err) {
        console.log('Topic was not found.');
        PythonShell.run('consumer.py', options, function (err, results){
          console.log('Running consumer.py script for subscribing to requested topic.');
          if (err){
            console.log('Error when running consumer.py script: ' + err);
          };
          console.log('results: %j', results);
        });
        res.send("The historical from the requested topic was not found. Please try again later and check the name of the topic if the error persists.");
      }else{
        console.log('Requested topic exists');
        res.sendFile(requestedTopicPath);
      }
     });
  });
