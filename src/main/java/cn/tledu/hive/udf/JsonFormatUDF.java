package cn.tledu.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import com.alibaba.fastjson.JSONObject;

public class JsonFormatUDF extends UDF {
	
	public String evaluate(String value){
		if(value == null){
			return null;
		}
		String[]keyValueArray = value.split("\001");
		if(keyValueArray == null || keyValueArray.length<2 || keyValueArray.length % 2 !=0){
			return null;
		}
		JSONObject jsonObject = new JSONObject();
		for(int i=0;i<keyValueArray.length;i += 2){
			jsonObject.put(keyValueArray[i], keyValueArray[i+1]);
		}
		return jsonObject.toString();
	}
	
	public static void main(String[] args){
		System.out.println(new JsonFormatUDF()
		 .evaluate("uid\001u1\001name\001昵称\001text\001你说的很对"));
	}
}
