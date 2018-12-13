package cn.tledu.utils;



import java.io.File;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;


/**
 * 封装的bdb操作工具类 集成了增、删、改、查、关闭、同步操作等方法
 */

public class BDBOperatorUtil {
	private String dbEnvFilePath;
	private String databaseName;
	//数据库操作对象的声明
	private Database weiboDatabase = null;
	//环境变量的声明
	private Environment myDbEnvironment = null;
	
	//bdb操作环境变量和数据库初始化
	public BDBOperatorUtil(String dbEnvFilePath,String databaseName){
		this.dbEnvFilePath = dbEnvFilePath;
		this.databaseName = databaseName;
		try{
			 // 初始化数据存储根目录文件夹
			File f = new File(dbEnvFilePath);
			if (!f.exists()) {
				f.mkdirs();
			}
			// 数据库配置变量初始化
			DatabaseConfig dbConfig = new DatabaseConfig();
			dbConfig.setAllowCreate(true);
			// 初始化环境配置变量，基于该变量去配置环境变量
			EnvironmentConfig envConfig = new EnvironmentConfig();
			envConfig.setAllowCreate(true);
			myDbEnvironment = new Environment(f, envConfig);
			weiboDatabase = myDbEnvironment.openDatabase(null, databaseName, dbConfig);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	 // 将指定的kv对存放到bdb当中，并可以选择是否实时同步到碰盘中
	public boolean put(String key,String value,boolean isSync){
		try {
			//将key和value都封装到DatabaseEntry中
			DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry theData = new DatabaseEntry(value.getBytes("UTF-8"));
			//写入数据库
			weiboDatabase.put(null, theKey, theData);
			if(isSync){
				//数据同步到磁盘当中
				this.sync();
			}
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	//删除bdb中指定的key值
	public boolean delete(String key){
		//执行删除操作
		try {
			DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
			weiboDatabase.delete(null, theKey);
			return true;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return false;
	}
	
	//读取bdb的key对应的数据
	public String getValue(String key){
		try {
			DatabaseEntry theKey = new DatabaseEntry(key.getBytes("UTF-8"));
			DatabaseEntry theData = new DatabaseEntry();
			//执行取操作
			weiboDatabase.get(null, theKey, theData, LockMode.DEFAULT);
			if (theData.getData() == null) {
		           return null;
		        }
			
		        // 将二进制数据转化成字符串值
			String result = new String(new String(theData.getData(),"utf-8"));
		    return result;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	  * 同步数据到碰盘当中，相当于让数据操作实时持久化
	  */
	public boolean sync(){
		if(myDbEnvironment != null){
			try {
				myDbEnvironment.sync();
				return true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return false;
	}
	
	//关闭环境变量和数据库
	public boolean close() {
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
		    return true;
		} catch (Exception e) {	
				e.printStackTrace();
		}
		return false;
	}
	
	public static void main(String[] args) {
		 // 数据库所在的存储文件夹
	      String dbEnvFilePath = "bdb2";
	      // 数据库名称
	      String databaseName = "weibo2";
	      String key = "self_key_1";
	      String value = "工具类操作示例";
	     
	      // 初始化
	      BDBOperatorUtil bdbUtil = new BDBOperatorUtil(dbEnvFilePath,
	           databaseName);
	      // 增加数据
//          bdbUtil.put(key, value, false); 
//        bdbUtil.sync();
//	      bdbUtil.delete(key);
	      System.out.println(bdbUtil.getValue(key));
		
	}
}
