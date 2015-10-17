/**
 * 
 */
package trident.stremhandler;

import java.util.HashMap;
import java.util.Map;

import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Values;

/** 
 * @ClassName: MyAggreater 
 * @Description:  
 * @date 2015-4-28 
 */
public class MyAggreater extends BaseAggregator<Map<String, Integer>> {

	public Map<String, Integer> init(Object batchId, TridentCollector collector) {
		return new HashMap<String, Integer>();
	}

	public void aggregate(Map<String, Integer> val, TridentTuple tuple, TridentCollector collector) {
		String str = tuple.getString(0);
		if (val.get(str) == null) {
			val.put(str, 1);
		} else {
			val.put(str, val.get(str) + 1);
		}
	}

	public void complete(Map<String, Integer> val, TridentCollector collector) {
		// TODO Auto-generated method stub
		collector.emit(new Values(val));
	}

	
}
