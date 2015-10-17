package storm;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt implements IBasicBolt {
	  
    private Map<String, Integer> _counts = new HashMap<String, Integer>();
    public void prepare(Map conf, TopologyContext context) {
        System.out.println("WordCountBolt=============="+conf.get("userName"));
    }
    
    public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            String stringByField = tuple.getStringByField("singleWord_key");
            String singleWordDesc_key = tuple.getStringByField("singleWordDesc_key");
            System.out.println(stringByField+" "+singleWordDesc_key);
            int count;
            if(_counts.containsKey(word)) {
                   count = _counts.get(word);
            } else {
                   count = 0;
            }
            count++;
            _counts.put(word, count);
            collector.emit(new Values(word, count));

     }

     public void cleanup() {

     }
     
     public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
     }
     
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}
}