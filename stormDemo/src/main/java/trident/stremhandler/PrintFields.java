/**
 * 
 */
package trident.stremhandler;

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
		   
		List<Object> values = tuple.getValues();
//		System.out.println(values);
		   collector.emit(new Values("008"));

		
	}
}