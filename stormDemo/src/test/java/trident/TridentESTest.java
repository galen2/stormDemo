package trident;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;
import trident.localStorm.Tweet;
import trident.localStorm.TweetBuilder;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.Document;
import com.github.fhuss.storm.elasticsearch.state.ESIndexMapState;

import functions.DocumentBuilder;

public class TridentESTest {
	public static class ExtractDocumentInfo extends BaseFunction {
		public void execute(TridentTuple tuple, TridentCollector collector) {
			Document t = (Document) tuple.getValue(0);//the cow jumped over the moon
			collector.emit(new Values(t.getId(), t.getName(), t.getType()));
		}
	}

	public static void main(String[] args) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3, new Values("the cow jumped over the moon"),
				new Values("the man went to the store and bought some candy"), new Values("four score and seven years ago"),
				new Values("how many apples can you eat"), new Values("to be or not to be the person"));
		spout.setCycle(true);

		TridentTopology topology = new TridentTopology();
		Settings settings = ImmutableSettings.settingsBuilder().loadFromClasspath("elasticsearch.yml").build();
		StateFactory stateFactory = ESIndexMapState.nonTransactional(new ClientFactory.Transport(settings.getAsMap()),
				Tweet.class);

		
		topology.newStream("tweets", spout).each(new Fields("sentence"), new DocumentBuilder(), new Fields("document"))
				
				.each(new Fields("document"), new ExtractDocumentInfo(), new Fields("id", "index", "type"))
				.groupBy(new Fields("index", "type", "id"))
				
				.persistentAggregate(stateFactory, new Fields("document"), new TweetBuilder(), new Fields("tweet"))
				.parallelismHint(1);

		Config config = new Config();
		config.put("storm.elasticsearch.cluster.name", "standalone");
		config.put("storm.elasticsearch.hosts", "172.16.2.205");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("trident", config, topology.build());
	}
}
