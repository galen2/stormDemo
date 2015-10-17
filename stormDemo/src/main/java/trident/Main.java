package trident;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import trident.stremhandler.PrintFields;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class Main {/*
	
	
	public static void main(String[] args) throws IOException {
		Main  main = new Main();
		main.demoOne();
	}
	
	
	public  void demoOne() {
		try {
			byte[] encoded = Files.readAllBytes(Paths.get("D:\\WorkSpace\\trident\\src\\main\\resources\\conf.json"));
			String contents = new String(encoded);
			JsonObject jo = GsonUtil.fromJson(contents, JsonObject.class);
			
			JsonArray ja1 = jo.get("lq_product").getAsJsonObject().get("t_product").getAsJsonObject().get("fields").getAsJsonArray();
			List<String> fields1 = new ArrayList<String>();
			for (JsonElement je: ja1) {
				fields1.add(je.getAsString());
			}
			
			BrokerHosts bhs = new ZkHosts("172.16.2.197:2181,172.16.2.197:3181,172.16.2.197:4181");
			TridentKafkaConfig tkc = new TridentKafkaConfig(bhs, "productdb_t_product");//订阅productdb_t_product的信息
			tkc.scheme = new SchemeAsMultiScheme(new JsonToMapScheme(fields1));
			
			OpaqueTridentKafkaSpout otks = new OpaqueTridentKafkaSpout(tkc);
			
			//提交Storm
			TridentTopology topology = new TridentTopology();
			Stream s1 = topology.newStream("spout1", otks);
			topology.merge(s1).each(new Fields("product_id"), new PrintFields(), new Fields("sentence1"));
			
			Config config = new Config();
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("trident", config, topology.build());
		} catch (Exception e) {
		}
	}
	
	
	public void demoTwo(){
		
		BrokerHosts hosts = new ZkHosts("");
		TridentKafkaConfig  kafkaConfig = new TridentKafkaConfig(hosts, "test-topic");
		kafkaConfig.scheme = new  SchemeAsMultiScheme(new JsonToMapScheme(""));
		
		
		OpaqueTridentKafkaSpout  kafkaSpout = new OpaqueTridentKafkaSpout(kafkaConfig);
		
		
		TridentTopology topology = new TridentTopology();
		Stream s1 = topology.newStream("stream1", kafkaSpout);
		topology.merge(s1).each(new Fields("product_id"), new PrintFields(), new Fields("sentence1"));

		
		LocalCluster cluster = new LocalCluster();
		Config config = new Config();
		cluster.submitTopology("trident", config, topology.build());
	}
*/}
