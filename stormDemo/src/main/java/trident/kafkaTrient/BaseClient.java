/**
 * 
 */
package trident.kafkaTrient;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

/** 
 * @ClassName: BaseClient 
 * @Description:  
 * @date 2015-4-28 
 */
public class BaseClient {

	static JsonObject jo  = null;
	
	static{
		try {
//			byte[] encoded = Files.readAllBytes(Paths.get("D:\\WorkSpace\\trident\\src\\main\\resources\\conf.json"));
			byte[] encoded = Files.readAllBytes(Paths.get("D:\\TrandingWork\\storm\\src\\main\\resources\\conf.json"));
			String contents = new String(encoded);
			jo = GsonUtil.fromJson(contents, JsonObject.class);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public List<String> getFieldOne(){
		JsonArray ja1 = jo.get("lq_product").getAsJsonObject().get("t_product").getAsJsonObject().get("fields").getAsJsonArray();
		List<String> fields1 = new ArrayList<String>();
		for (JsonElement je: ja1) {
			fields1.add(je.getAsString());
		}
		return fields1;
	}
}
