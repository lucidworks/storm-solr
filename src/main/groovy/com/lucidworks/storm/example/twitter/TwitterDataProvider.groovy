package com.lucidworks.storm.example.twitter

import twitter4j.FilterQuery
import twitter4j.StallWarning
import twitter4j.Status
import twitter4j.StatusDeletionNotice
import twitter4j.StatusListener
import twitter4j.TwitterStream
import twitter4j.TwitterStreamFactory

import com.lucidworks.storm.spring.NamedValues
import com.lucidworks.storm.spring.StreamingDataProvider
import org.apache.log4j.Logger

import backtype.storm.utils.Utils

import java.util.concurrent.LinkedBlockingQueue

class TwitterDataProvider implements StreamingDataProvider {

  static Logger log = Logger.getLogger(TwitterDataProvider)

  LinkedBlockingQueue<Status> queue
  String[] keywords

  void setKeywords(String[] keywords) {
    this.keywords = keywords
  }

  @Override
  void open(Map stormConf) {
    queue = new LinkedBlockingQueue<Status>(1000)

    StatusListener listener = new StatusListener() {

      @Override
      public void onStatus(Status status) {
        queue.offer(status)
      }

      @Override
      public void onDeletionNotice(StatusDeletionNotice sdn) {
      }

      @Override
      public void onTrackLimitationNotice(int i) {
      }

      @Override
      public void onScrubGeo(long l, long l1) {
      }

      @Override
      public void onException(Exception ex) {
      }

      @Override
      public void onStallWarning(StallWarning arg0) {
      }
    };

    TwitterStream twitterStream = new TwitterStreamFactory().getInstance()
    twitterStream.addListener(listener)
    // credentials loaded from twitter4j.properties on the classpath
    if (keywords != null && keywords.length == 0) {
      twitterStream.filter(new FilterQuery().track(keywords))
    } else {
      twitterStream.sample()
    }
  }

  @Override
  boolean next(NamedValues record) throws Exception {
    Status status = queue.poll();
    if (status) {
      record.set("id", "tweet-"+status.getId())
      record.set("tweet", status)
      return true
    }

    Utils.sleep(50)
    return false
  }
}
