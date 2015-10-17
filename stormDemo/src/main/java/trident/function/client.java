package trident.function;

import storm.trident.TridentTopology;
import storm.trident.testing.FixedBatchSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class client {

	public static void main(String[] args) {
		FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
	               new Values("the cow jumped over the moon"),
	               new Values("the man went to the store and bought some candy"),
	               new Values("four score and seven years ago"),
	               new Values("how many apples can you eat"));
		spout.setCycle(true);
		
		TridentTopology topology = new TridentTopology();        
//		TridentState wordCounts =
	     topology.newStream("spout1", spout)
	       .each(new Fields("sentence"), new SplitFunction(), new Fields("word_one","word_one_two"))
	       .each(new Fields("word_one","word_one_two") , new PrintFields(),new Fields("word_two"))
	       .each(new Fields("word_two") , new functionTwo(),new Fields("word_three"));
			       
			    /*   .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))                
			       .parallelismHint(6);*/
			
			Config config = new Config();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trident", config, topology.build());
	}
}
