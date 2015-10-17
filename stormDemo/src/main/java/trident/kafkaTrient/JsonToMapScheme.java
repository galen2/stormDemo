package trident.kafkaTrient;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;


/*
 * 把从kafka获取的某一条JSON数据转换为MAP对象
 */
public class JsonToMapScheme implements Scheme {

	private List<String> fields;

	private final static class JsonToMap {
		private final static Gson gson = new Gson();
		private final static Type type = new TypeToken<Map<String, String>>() {
		}.getType();

		static Map<String, String> convert(String json) {
			JsonObject jo = gson.fromJson(json, JsonObject.class);
			String ot = jo.get("ot").getAsString();
			Map<String, String> map = gson.fromJson(gson.toJson(jo.get("colmunsValue")), type);
			map.put("_ot_", ot);
			return map;
		}

		static Map<String, String> convert(byte[] bytes) {
			return convert(new String(bytes));
		}
	}

	public JsonToMapScheme(List<String> fields) {
		this.fields = fields;
	}
	
	public JsonToMapScheme(String... fields) {
		this(Arrays.asList(fields));
	}

	/**
	 * 获取KAFA获取的某一条数据的JSON数据
	 */
	public List<Object> deserialize(byte[] ser) {

		Map<String, String> map = JsonToMap.convert(ser);

		List<Object> values = new ArrayList<Object>();
		for (String field: fields) {
			if (map.containsKey(field)) {
				values.add(map.get(field));
			} else {
				// 输入数据如果缺少某些Field，会导致Trident异常，所以，这里加入默认的空字符串
				values.add("");
			}
		}
//		for (Entry<String, String> entry : map.entrySet()) {
//			if (fields.contains(entry.getKey())) {
//				System.out.printf("%s: %s\n", entry.getKey(), entry.getValue());
//				values.add(entry.getValue());
//			} else {
//				System.out.printf("NOT: %s: %s\n", entry.getKey(), entry.getValue());
//			}
//		}
		System.out.print(values.toString());
		return values;
	}

	public Fields getOutputFields() {
		return new Fields(fields);
	}

}
