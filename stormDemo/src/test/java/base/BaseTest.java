/**
 * 
 */
package base;

import org.junit.After;


/** 
 * @ClassName: BaseTest 
 * @Description:  
 * @date 2015-4-28 
 */
public class BaseTest {
	
	@After
	public void after(){
		synchronized (this) {
			try {
				wait();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
}
