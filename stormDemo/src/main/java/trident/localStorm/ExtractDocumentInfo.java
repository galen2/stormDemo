/**
 * 
 */
package trident.localStorm;

import java.util.List;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import com.github.fhuss.storm.elasticsearch.Document;

/** 
 * @ClassName: ExtractDocumentInfo 
 * @Description:  
 * @date 2015-4-28 
 */
public class ExtractDocumentInfo  extends BaseFunction {
	public void execute(TridentTuple tuple, TridentCollector collector) {
		Fields fields = tuple.getFields();
    	System.out.println(fields);
    	
		
		List<Object> values = tuple.getValues();
		System.out.println(tuple.getValue(0));
		System.out.println(tuple.getValue(0) instanceof Document);
		System.out.println(values);
		Document t = (Document) tuple.getValue(0);//the cow jumped over the moon
		collector.emit(new Values(t.getId(), t.getName(), t.getType()));
	}
}

