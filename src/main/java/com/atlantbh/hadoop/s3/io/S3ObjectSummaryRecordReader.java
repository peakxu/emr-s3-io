package com.atlantbh.hadoop.s3.io;

import org.apache.hadoop.io.Text;

public class S3ObjectSummaryRecordReader extends S3RecordReader<Text, S3ObjectSummaryWritable> {

	public S3ObjectSummaryRecordReader() {
	}

	@Override
	protected void setNextKeyValue(Text text, S3ObjectSummaryWritable s3ObjectSummaryWritable) {
		text.set(String.format("%s/%s", currentKey.getBucketName(), currentKey.getKey()));

		s3ObjectSummaryWritable.setBucketName(currentKey.getBucketName());
		s3ObjectSummaryWritable.setKey(currentKey.getKey());
		s3ObjectSummaryWritable.setETag(currentKey.getETag());
		s3ObjectSummaryWritable.setLastModified(currentKey.getLastModified());
		s3ObjectSummaryWritable.setOwner(currentKey.getOwner());
		s3ObjectSummaryWritable.setSize(currentKey.getSize());
		s3ObjectSummaryWritable.setStorageClass(currentKey.getStorageClass());
	}

	public Text createKey() {
		return new Text();
	}

	public S3ObjectSummaryWritable createValue() {
		return new S3ObjectSummaryWritable();
	}
}
