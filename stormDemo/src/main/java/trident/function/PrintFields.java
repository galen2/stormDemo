/**
 * 
 */
package trident.function;

import java.util.Iterator;
import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

/** 
 * @ClassName: PrintFields 
 * @Description:  
 * @date 2015-4-28 
 */
public class PrintFields extends BaseFunction {
	
	public void execute(TridentTuple tuple, TridentCollector collector) {
		   Fields fields = tuple.getFields();
		   System.out.println(fields);
		   Object valueByField = tuple.getValueByField("word_one");
		   System.out.println(valueByField);
		   
		   Object word_one_two = tuple.getValueByField("word_one_two");
		   System.out.println(word_one_two);
		List<Object> values = tuple.getValues();
//		System.out.println(values);
		   collector.emit(new Values("008"));

		
	}
}