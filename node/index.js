var app = require('express')();
var http = require('http').Server(app);
var io = require('socket.io')(http);
var kafka = require('kafka-node');
var HighLevelConsumer = kafka.HighLevelConsumer;
var Offset = kafka.Offset;
var Client = kafka.Client;
var topic = 'parking-test-summary';
var client = new Client('34.209.151.179:2181', "worker-" + Math.floor(Math.random() * 10000));
var payloads = [{ topic: topic }];
var consumer = new HighLevelConsumer(client, payloads);
var offset = new Offset(client);
var port = 3001;
var filter_topic = ""

app.get('/', function(req, res){
    //res.sendFile('../index.html');
    res.sendFile('index.html', { root: __dirname });
});

io = io.on('connection', function(socket){
    console.log('a user connected');
    socket.on('disconnect', function(){
        console.log('user disconnected');
    });

    socket.on('topic', function (data) {
        console.log("Messages filtered to " + data)
        filter_topic = data
    })

});



consumer = consumer.on('message', function(message) {
    console.log(message.value);
    io.emit("message", message.value);
});

http.listen(port, function(){
    console.log("Running on port " + port)
});