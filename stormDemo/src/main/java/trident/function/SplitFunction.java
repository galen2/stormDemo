package trident.function;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class SplitFunction extends BaseFunction {
	   public void execute(TridentTuple tuple, TridentCollector collector) {
		   Fields fields = tuple.getFields();
		   System.out.println(fields);
		   
			
		   List<Object> values = tuple.getValues();
		   System.out.println(values);

//		   collector.emit(new Values(tuple.getString(0).toUpperCase()));
		   collector.emit(new Values("007"));
		
	   }
	}
