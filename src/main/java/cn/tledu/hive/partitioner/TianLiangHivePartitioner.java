package cn.tledu.hive.partitioner;

import org.apache.hadoop.hive.ql.io.HivePartitioner;
import org.apache.hadoop.mapred.lib.HashPartitioner;


public class TianLiangHivePartitioner<K2, V2> extends HashPartitioner<K2, V2>
		implements HivePartitioner<K2, V2> {
	
	@Override
	   public int getPartition(K2 key, V2 value, int numReduceTasks) {
	     return (key.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
	   }

	@Override
	public int getBucket(K2 arg0, V2 arg1, int arg2) {
		return 0;
	}

	

}
