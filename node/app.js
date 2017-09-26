var kafka = require('kafka-node'),
    Consumer = kafka.Consumer,
    client = new kafka.Client('34.209.151.179:9091', "worker-" + Math.floor(Math.random() * 10000)),
    consumer = new Consumer(
        client,
        [
            { topic: 'parking-test-summary'}
        ],
        {
            autoCommit: false
        }
    );

consumer.on('message', function (message) {
    console.log(message);
});