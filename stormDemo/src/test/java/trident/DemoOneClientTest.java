/**
 * 
 */
package trident;

import org.junit.Before;
import org.junit.Test;

import trident.kafkaTrient.DemoOneClient;
import base.BaseTest;

/** 
 * @ClassName: DemoOneClientTest 
 * @Description:  
 * @date 2015-4-28 
 */
public class DemoOneClientTest extends BaseTest {
	
	DemoOneClient one = null;
	
	@Before
	public void setUp(){
		one = new DemoOneClient();
	}
	
	@Test
	public void runOne(){
		one.demoOne();
	}
	
}
