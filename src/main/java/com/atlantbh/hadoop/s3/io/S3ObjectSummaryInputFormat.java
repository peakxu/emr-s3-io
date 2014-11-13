package com.atlantbh.hadoop.s3.io;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

/**
 * S3 object summary input format
 * 
 * @author seljaz
 *
 */
public class S3ObjectSummaryInputFormat extends S3InputFormat<Text, S3ObjectSummaryWritable>  {

	public S3ObjectSummaryInputFormat() throws IOException {
		super();
	}

	@Override
	public RecordReader<Text, S3ObjectSummaryWritable> getRecordReader(InputSplit inputSplit, JobConf jobConf, Reporter reporter) throws IOException {
		return new S3ObjectSummaryRecordReader();
	}
}
