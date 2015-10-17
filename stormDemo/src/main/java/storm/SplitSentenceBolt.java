package storm;

import java.util.Map;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SplitSentenceBolt implements IBasicBolt {

	/**
	 * conf有客户端设置： cluster.submitTopology("test", conf,
	 * builder.createTopology());
	 */
	public void prepare(Map conf, TopologyContext context) {
		System.out.println(conf.get("userName"));
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String sentence = tuple.getString(0);
		for (String word : sentence.split(" ")) {
			collector.emit(new Values(word, word + "Desc"));
		}
	}

	public void cleanup() {
	}

	/**
	 * 生命execute方法emit内容值得key分别是什么
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// declarer.declareStream(streamId, fields)
		declarer.declare(new Fields("singleWord_key", "singleWordDesc_key"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}
}
