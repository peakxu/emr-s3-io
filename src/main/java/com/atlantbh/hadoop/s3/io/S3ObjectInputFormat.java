package com.atlantbh.hadoop.s3.io;

import java.io.IOException;

import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

/**
 * S3 object input format. The purpose of this input format class is to read keys from Amazon S3 service 
 * in form of (key, value) = ({@link S3ObjectSummaryWritable}, {@link S3ObjectWritable}) 
 * 
 * @author seljaz
 *
 */
public class S3ObjectInputFormat extends S3InputFormat<S3ObjectSummaryWritable, S3ObjectWritable>  {

	public S3ObjectInputFormat() throws IOException {
		super();
	}

	@Override
	public org.apache.hadoop.mapred.RecordReader<S3ObjectSummaryWritable, S3ObjectWritable> getRecordReader(org.apache.hadoop.mapred.InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
		return new S3ObjectRecordReader();
	}
}