/**
 * 
 */
package trident.stremhandler;

import java.util.Map;

import storm.trident.operation.BaseFilter;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

/** 
 * @ClassName: MyFilter 
 * @Description:  
 * @date 2015-4-28 
 */
public class MyFilter extends BaseFilter {
	
	int partion_idx;
	public void prepare(Map conf, TridentOperationContext context) {
		this.partion_idx = context.getPartitionIndex();
    }

	public boolean isKeep(TridentTuple tuple) {
		if (tuple.getString(0).equals("the man went to the store and bought some candy") == true) {
			System.out.println("Filted by " + partion_idx);
			return false;
		} else {
			return true;
		}
	}
	
}