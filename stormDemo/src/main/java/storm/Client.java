/**
 * 
 */
package storm;

import java.io.Serializable;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

/** 
 * @ClassName: Client 
 * @Description:  
 * @date 2015-4-29 
 */
public class Client implements Serializable{
	
	public static void main(String[] args) {
		Client  client = new Client();
		splitWordSpolt spout = new splitWordSpolt();


		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("1_spout",spout).setNumTasks(3);
		builder.setBolt("2_bolt", new SplitSentenceBolt(), 1).shuffleGrouping("1_spout");
		
		//会按照某个单词的固定算法，把每个单词分配到固定的bolt上面
//		builder.setBolt("3_bolt", new WordCountBolt(), 2).fieldsGrouping("2_bolt", new Fields("singleWord_key"));
		
//		declareOutputFields不需要声明
//		builder.setBolt("3_bolt", client.new WordCount(), 2).shuffleGrouping("2_bolt");
		
	    Config conf = new Config();
        conf.setDebug(true);
        conf.put("userName", "张三");
        
        conf.setNumWorkers(1);
        conf.setMaxSpoutPending(5000);
        
        /*
         * 提交到集群的方式
        try {
			StormSubmitter.submitTopology("mytopology", conf, builder.createTopology());
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		}*/
        
        //本地调试模式
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
/*         Utils.sleep(10000);
         cluster.killTopology("test");
         cluster.shutdown();*/
         
	}

}
