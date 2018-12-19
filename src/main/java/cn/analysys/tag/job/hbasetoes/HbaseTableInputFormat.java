package cn.analysys.tag.job.hbasetoes;

import org.apache.hadoop.hbase.mapred.TableInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;

public class HbaseTableInputFormat extends TableInputFormat{

	@Override
	public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
		// TODO Auto-generated method stub
		return super.getSplits(job, 1530);
	}
	
}
