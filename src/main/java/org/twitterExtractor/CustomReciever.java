package org.twitterExtractor;

/**
 * Created by Martin Anderson
 */

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;
import twitter4j.auth.AccessToken;

import static com.google.common.base.Preconditions.checkArgument;

/**
This class look for status messages containing the required keywords
 */
public final class CustomReciever extends Receiver<Status> {
  private static final Logger printer = LoggerFactory.getLogger(CustomReciever.class);


  private final TwitterStream stream;
  private StatusListener statusListener;
  // which word are you looking for?
  private static final String searchThis = "twitter";
  // put your credentials here
  private String token = "";
  private String tokenSecret = "";
  private String consumerKey = "";
  private String consumerSecret = "";
  private AccessToken accessToken;



  public CustomReciever(StorageLevel storageLevel) {
    super(storageLevel);
    accessToken = new AccessToken(token, tokenSecret);
    checkArgument(StorageLevel.MEMORY_ONLY().equals(storageLevel),
        String.format("Only [%s] supported.", StorageLevel.MEMORY_ONLY().toString()));

    stream = new TwitterStreamFactory().getInstance();
    stream.setOAuthConsumer(consumerKey, consumerSecret);
    stream.setOAuthAccessToken(accessToken);


  }

  @Override
  public void onStart() {
    if (statusListener == null) {
      statusListener = new myStatusListner();
    }
    stream.addListener(statusListener);
    stream.filter(makeNewFilter());
  }

  private FilterQuery makeNewFilter() {
    FilterQuery fQuery = new FilterQuery();
    try {
      fQuery.track(searchThis);
    } catch (Exception e) {
      printer.error(e.getMessage(), e);
      throw new IllegalArgumentException(e);
    }
    return fQuery;
  }

  @Override
  public void onStop() {
    stream.clearListeners();
    stream.cleanUp();
    statusListener = null;
  }


  public class myStatusListner implements StatusListener {
    @Override
    public void onStatus(Status status) {
      store(status);
    }

    @Override
    public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {

    }

    @Override
    public void onTrackLimitationNotice(int numberOfLimitedStatuses) {

    }

    @Override
    public void onScrubGeo(long userId, long upToStatusId) {

    }

    @Override
    public void onStallWarning(StallWarning warning) {

    }

    @Override
    public void onException(Exception ex) {
      printer.warn(ex.getMessage(), ex);
    }
  }

}
