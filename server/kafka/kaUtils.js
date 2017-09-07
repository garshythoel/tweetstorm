/* Avro begin */
const avroSchema = {
   name: 'TweetIn',
   type: 'record',
   fields: [
     {
       name: 'tweet_id',
       type: 'string'
     }, {
       name: 'content',
       type: 'string'
     }, {
       name: 'user_name',
       type: 'string'
     }, {
       name: 'location',
       type: 'string'
     }]
 };

const avro = require('avsc');
const type = avro.parse(avroSchema);
/* Avro end */

const createPayload = (tweet) => {
  const buffer = type.toBuffer({
    tweet_id: tweet.id_str,
    content: tweet.text,
    user_name: tweet.user.name || '',
    location: tweet.user.location || ''
  });

  return [{
    topic: 'tweet-stream',
    messages: buffer,
    attributes: 1 /* Use GZip compression for the payload */
  }];
};

const parseMessage = (buf) => {
  return type.fromBuffer(buf);
};

module.exports = {
  createPayload,
  parseMessage
};
