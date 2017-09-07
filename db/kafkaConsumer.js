const kafka = require('kafka-node');
const kaUtils = require('../server/kafka/kaUtils');
const HighLevelConsumer = kafka.HighLevelConsumer;
const Client = kafka.Client;

const client = new Client('localhost:2181');
const topics = [{
  topic: 'tweet-stream'
}];

const options = {
  autoCommit: true,
  fetchMaxWaitMs: 1000,
  fetchMaxBytes: 1024 * 1024,
  encoding: 'buffer'
};
const consumer = new HighLevelConsumer(client, topics, options);

const connectionString = 'postgres://localhost:5432/tweedb';
const pgp = require('pg-promise')({});
const db = pgp(connectionString);

consumer.on('message', async function(message) {
  let buf = new Buffer(message.value, 'binary');
  let event = kaUtils.parseMessage(buf);
  try {
      let dbresp = await db.query(
          "INSERT INTO tweet(tweet_id, content, user_name, location) VALUES(${id}, ${content}, ${user}, ${location}) RETURNING id",
          {
            id: event.tweet_id,
            content: event.content,
            user: event.user_name || '',
            location: event.location || ''
          }
      );
      console.log(dbresp);
  } catch (err) {
    console.log('Shit broke: ', err);
    res.sendStatus(500);
  }
});

consumer.on('error', function(err) {
  console.log('error', err);
});

process.on('SIGINT', function() {
  consumer.close(true, function() {
    process.exit();
  });
});
