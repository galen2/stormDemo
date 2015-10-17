/**
 * 
 */
package trident.kafkaTrient;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.kafka.trident.TridentKafkaState;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import trident.stremhandler.PrintFields;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;



/** 
 * @ClassName: DemoOneClient 
 * @Description:  
 * @date 2015-4-28 
 */
public class DemoOneClient extends BaseClient{
	
	public  void demoOne() {
		try {
			List<String> fields1 = super.getFieldOne();
			BrokerHosts bhs = new ZkHosts("172.16.2.197:2181,172.16.2.197:3181,172.16.2.197:4181");
			TridentKafkaConfig tkc = new TridentKafkaConfig(bhs, "productdb_t_product");//订阅productdb_t_product的信息
			tkc.scheme = new SchemeAsMultiScheme(new JsonToMapScheme(fields1));
			
			OpaqueTridentKafkaSpout otks = new OpaqueTridentKafkaSpout(tkc);
			
			//提交Storm
			TridentTopology topology = new TridentTopology();
			Stream s1 = topology.newStream("spout1", otks);
//			topology.merge(s1).each(new Fields("product_id"), new PrintFields(), new Fields("sentence1"));
			topology.merge(s1).each(new Fields("product_name","product_id"), new PrintFields(), new Fields("sentence1"));
			
			Config config = new Config();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trident", config, topology.build());
		} catch (Exception e) {
		}
	}
	
	
	public void demoTwo(){
		try {
			List<String> fields1 = super.getFieldOne();
//			BrokerHosts bhs = new ZkHosts("172.16.2.197:2181");
			
			String topicName  = "productdb_t_product";
			BrokerHosts hosts = new ZkHosts("172.16.2.197:2181");
			SpoutConfig config = new SpoutConfig(hosts, topicName, "/" + topicName, UUID.randomUUID().toString());
			config.scheme = new SchemeAsMultiScheme(new StringScheme());
			KafkaSpout kafkaSpout = new KafkaSpout(config);
			
			TopologyBuilder builder = new TopologyBuilder();
	        builder.setSpout("spout", kafkaSpout, 5);

	        Config conf = new Config();
	        //set producer properties.
	        Properties props = new Properties();
//	        props.put("metadata.broker.list", "localhost:9092");
	        props.put("request.required.acks", "1");
	        props.put("serializer.class", "kafka.serializer.StringEncoder");
	        conf.put(TridentKafkaState.KAFKA_BROKER_PROPERTIES, props);

	        
	        LocalCluster cluster = new LocalCluster();
//	        cluster.submitTopology("test", conf, builder.createTopology());
	        cluster.submitTopology("kafkaboltTest", conf, builder.createTopology());
	        
	        
	        /*KafkaBolt bolt = new KafkaBolt()
            .withTopicSelector(new DefaultTopicSelector("test"))
            .withTupleToKafkaMapper(new FieldNameBasedTupleToKafkaMapper());
	        builder.setBolt("forwardToKafka", bolt, 8).shuffleGrouping("spout");*/
    
			
			
			
			/*//提交Storm
			TridentTopology topology = new TridentTopology();
			Stream s1 = topology.newStream("spout1", kafkaSpout);
			topology.merge(s1).each(new Fields("product_name","product_id"), new PrintFields(), new Fields("sentence1"));
			Config config = new Config();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trident", config, topology.build());*/
		} catch (Exception e) {
			// TODO: handle exception
		}
		
	}
	
	public static void main(String[] args) {
		DemoOneClient  main = new DemoOneClient();
//		main.demoOne();
		main. demoTwo();
	}
	
}
