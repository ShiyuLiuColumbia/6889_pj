//receive data directly from kafka without redis

var argv = require('minimist')(process.argv.slice(2));
var port = argv['port'];
var price_topic = argv['price_topic']
var kafka_topic = argv['kafka_topic'];
var kafka_window_topic = argv['kafka_window_topic'];
var kafka_broker = argv['kafka_broker'];


// - setup dependency instances
var express = require('express');
var app = express();
var server = require('http').createServer(app);
var io = require('socket.io')(server);


// - setup kafka consumer
var kafka = require('kafka-node');
var Consumer = kafka.Consumer;
// var Client = kafka.Client;
var client = new kafka.KafkaClient();
console.log('Creating a kafka consumer');
var consumer = new Consumer(
		client,
		[{topic:kafka_topic}, {topic:kafka_window_topic},{topic:price_topic}]
	);
console.log('Subscribing to kafka topic %s and %s', kafka_topic, kafka_window_topic);
consumer.on('message',function(message){
    console.log('Received new data from kafka %s',JSON.stringify(message));
    io.sockets.emit('data', JSON.stringify(message));	
})





// - setup webapp routing
app.use(express.static(__dirname + '/public/'));
console.log(__dirname + '/node_modules/jquery/dist/');
app.use('/jquery', express.static(__dirname + '/node_modules/jquery/dist/'));
app.use('/d3', express.static(__dirname + '/node_modules/d3'));
app.use('/nvd3', express.static(__dirname + '/node_modules/nvd3/build/'));
app.use('/bootstrap', express.static(__dirname + '/node_modules/bootstrap/dist'));
// app.use((req, res, next) => {
//     console.log(req);
//     res.header("Access-Control-Allow-Origin", "*");
//     res.header(
//       "Access-Control-Allow-Headers",
//       "Origin, X-Requested-With, Content-Type, Accept, Authorization"
//     );
//     if (req.method === "OPTIONS") {
//       res.header("Access-Control-Allow-Methods", "PUT, POST, PATCH, DELETE, GET");
//       return res.status(200).json({});
//     }
//     next();
//   });
  
server.listen(port, function () {
    console.log('Server started at port %d.', port);
});

// - setup shutdown hooks
var shutdown_hook = function () {
    console.log('Quitting Kafka consumer');
    consumer.close();
    console.log('Shutting down app');
    process.exit();
};

process.on('SIGTERM', shutdown_hook);
process.on('SIGINT', shutdown_hook);
process.on('exit', shutdown_hook);