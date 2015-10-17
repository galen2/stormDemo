package trident.localStorm;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;

import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.fhuss.storm.elasticsearch.ClientFactory;
import com.github.fhuss.storm.elasticsearch.state.ESIndexMapState;

public class localStormSpolt {

	public static void main(String[] args) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence_inputFiled"), 3, 
				new Values("the cow jumped over the moon"),
				new Values("the man went to the store and bought some candy"),
				new Values("four score and seven years ago"),
				new Values("how many apples can you eat"), 
				new Values("to be or not to be the person"));
		spout.setCycle(true);

		TridentTopology topology = new TridentTopology();
		Settings settings = ImmutableSettings.settingsBuilder().loadFromClasspath("elasticsearch.yml").build();
		StateFactory stateFactory = ESIndexMapState.nonTransactional(new ClientFactory.Transport(settings.getAsMap()),
				Tweet.class);
		
		topology.newStream("tweets", spout)
				.each(new Fields("sentence_inputFiled"), new DocumentBuilder(), new Fields("document_functionFiled"))
				.each(new Fields("document_functionFiled"), new ExtractDocumentInfo(), new Fields("id", "index", "type")).groupBy(new Fields("index", "type", "id"))
				.persistentAggregate(stateFactory, new Fields("document_functionFiled"), new TweetBuilder(), new Fields("tweet")).parallelismHint(1);

		Config config = new Config();
		config.put("storm.elasticsearch.cluster.name", "standalone");
		config.put("storm.elasticsearch.hosts", "172.16.2.205");
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("trident", config, topology.build());
	}
}
