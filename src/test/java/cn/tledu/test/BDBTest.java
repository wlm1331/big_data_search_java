package cn.tledu.test;

import java.io.File;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;

public class BDBTest {
	public static void main(String[] args) {
		/**
         * 初始化数据库参数
         */
		String dbEnvFilePath = "bdb";
		String database = "weibo";
		Environment myDbEnvironment = null;
		Database weiboDatabase = null;
		try{
		File f = new File(dbEnvFilePath);
		if (!f.exists()) {
			f.mkdirs();
		}
		DatabaseConfig dbConfig = new DatabaseConfig();
		dbConfig.setAllowCreate(true);
		EnvironmentConfig envConfig = new EnvironmentConfig();
		envConfig.setAllowCreate(true);
		myDbEnvironment = new Environment(f, envConfig);
		weiboDatabase = myDbEnvironment.openDatabase(null, database, dbConfig);
	}catch(Exception e){
		e.printStackTrace();
	}
	
	//存储数据
	String aKey = "key1";
	String aData = "data";
	try {
		//将key和value都封装到DatabaseEntry中
		DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));
		DatabaseEntry theData = new DatabaseEntry(aData.getBytes("UTF-8"));
		//写入数据库
		weiboDatabase.put(null, theKey, theData);
		//对该库进行count操作，查找有多少条数据
		System.out.println(weiboDatabase.count());
	} catch (Exception e) {
		e.printStackTrace();
	}
	
	//读取数据
	aKey = "key1";
	try {
		DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));
		DatabaseEntry theData = new DatabaseEntry();
		//取操作
		weiboDatabase.get(null, theKey, theData, LockMode.DEFAULT);
	    String result = new String(theData.getData(),"UTF-8");
	    System.out.println(result);
	} catch (Exception e) {
		e.printStackTrace();
	}
	
	//删除数据
	aKey = "key1";
	try {
		DatabaseEntry theKey = new DatabaseEntry(aKey.getBytes("UTF-8"));
		weiboDatabase.delete(null, theKey);
		System.out.println(weiboDatabase.count());
	} catch (Exception e) {
		e.printStackTrace();
	}
	
	//关闭
	try {
		//先关闭数据库
	    if(weiboDatabase != null){
			 weiboDatabase.close();
	    }
	    //再关闭BDB系统环境变量
	    if(myDbEnvironment != null){
	    	myDbEnvironment.sync();
	    	myDbEnvironment.cleanLog();
	    	myDbEnvironment.close();
	    }
	} catch (Exception e) {	
			e.printStackTrace();
	}
  }	
}

