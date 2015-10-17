package trident.localStorm;


import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.github.fhuss.storm.elasticsearch.Document;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DocumentBuilder extends BaseFunction {
    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
    	Fields fields = tuple.getFields();
    	System.out.println(fields);
    	
        String sentence = tuple.getString(0);
        System.out.println(sentence);
        Document docu = new Document("aa", "bb", "cc");
        collector.emit(new Values(docu));
    }
}
