package trident.kafkaTrient;

import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;

/**
 * Created by Administrator on 2014/7/31.
 */
public class GsonUtil {
	
	private static GsonBuilder gb;
	static{
		gb = new GsonBuilder();
		gb.disableHtmlEscaping();
		gb.serializeNulls();
		
	}

    public static Map<String,Object> json2Map(String json){
        GsonBuilder gb = new GsonBuilder();
        Gson g = gb.create();
        Map<String, Object> map = g.fromJson(json, new TypeToken<Map<String, Object>>() {}.getType());
        return map;
    }
    
    public static JsonElement getJsonElement(String json){
    	return new Gson().fromJson(json, JsonElement.class);
    }
    
    public static String toJson(Object je){
    	return gb.create().toJson(je);
    }

	public static <T> T fromJson(String json,Class<T> t) {
		return (T) gb.create().fromJson(json, t);
	}

}
