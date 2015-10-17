/**
 * 
 */
package trident.localStorm;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

import com.github.fhuss.storm.elasticsearch.Document;

/** 
 * @ClassName: TweetBuilder 
 * @Description:  
 * @date 2015-4-28 
 */
public class TweetBuilder implements ReducerAggregator<Tweet> {
    @Override
    public Tweet init() {
        return null;
    }

    @Override
    public Tweet reduce(Tweet tweet, TridentTuple objects) {

        Document<String> doc  = (Document) objects.getValueByField("document");
        if( tweet == null)
            tweet = new Tweet(doc.getSource(), 1);
        else {
            tweet.incrementCount();
        }

        return tweet;
    }
}
