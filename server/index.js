console.log('Spinning up...');

/* Express Server */
const express = require('express');
const path = require('path');
const app = express();
const BASE_URL = 'localhost:3000/';

/* Twitter client */
const twitter = require('twitter');
var twitClient = new twitter({
  consumer_key: '4qmTjG0CXmqWtJ9sb1C0vEw7X',
  consumer_secret: 'DzUjiSAsvrUKgZKcErExFU0exwtI4oDO0LMbFu1C6DhMQybpnA',
  access_token_key: '2457027740-GgWZwZVfDvQ20oY2zo7FoKyqunlsoHs4BfBxdvL',
  access_token_secret: '9xqK2Jdg3uCGenRUgdwXwALU1m4XNppo9262biEeFGRUH'
});

/* Kafka Producer */
const kaUtils = require('./kafka/kaUtils');
const kafka = require('kafka-node');
const HighLevelProducer = kafka.HighLevelProducer;
const KeyedMessage = kafka.KeyedMessage;
const Client = kafka.Client;
const client = new Client('localhost:2181', 'tweet_stormer_prod_1', {
  sessionTimeout: 300,
  spinDelay: 100,
  retries: 2
});
client.on('ready', function (){
  console.log('client ready');
});

client.on('error', function(error) {
  console.error(error);
});
const producer = new HighLevelProducer(client);

producer.on('ready', () => {
  var stream = twitClient.stream('statuses/filter', {track: 'kim jong un'});
  stream.on('data', async function(event) {
    /* stream to kafka */
    let kaPayload = kaUtils.createPayload(event);
    console.log('ready');
    //Send payload to Kafka and log result/error
    producer.send(kaPayload, function(error, result) {
      console.info('Sent payload to Kafka: ', event.id_str);
      if (error) {
        console.error(error);
      } else {
        const formattedResult = result[0]
        console.log('result: ', result)
      }
    });

  });

  stream.on('error', function(error) {
      throw error;
  });
})

producer.on('error', function(error) {
  console.error(error);
});

app.use(async (req, res) => {

});

app.get('/', async (req, res) => {
  console.log('Server is ALIVE');
  res.sendStatus(200);
});

app.listen(3000);
