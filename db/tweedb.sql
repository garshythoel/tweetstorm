DROP DATABASE IF EXISTS tweedb;
CREATE DATABASE tweedb;

\c tweedb;

CREATE TABLE tweet (
  ID SERIAL PRIMARY KEY,
  TWEET_ID TEXT,
  CONTENT TEXT,
  URL TEXT,
  USER_NAME TEXT,
  LOCATION TEXT
);

INSERT INTO tweet(tweet_id, content, url, user_name, location)
  VALUES ('0' , 'hey', 'www.tweet.com', 'tweety mctwitface', 'tweetistan');
