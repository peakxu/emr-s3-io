package com.atlantbh.hadoop.s3.io;

import java.io.IOException;

import com.amazonaws.services.s3.model.S3Object;
import org.apache.hadoop.io.Text;

/**
 * Record reader for reading ({@link S3ObjectSummaryWritable}, {@link S3ObjectWritable}) as (key, value) pairs
 * from underlying S3 Input Split
 *
 * @author seljaz
 *
 */
public class S3ObjectRecordReader extends S3RecordReader<S3ObjectSummaryWritable, S3ObjectWritable> {
	S3Object object;

	public S3ObjectRecordReader() {
	}

	@Override
	protected void setNextKeyValue(S3ObjectSummaryWritable s3ObjectSummaryWritable, S3ObjectWritable s3ObjectWritable) {
		s3ObjectSummaryWritable.setBucketName(currentKey.getBucketName());
		s3ObjectSummaryWritable.setKey(currentKey.getKey());
		s3ObjectSummaryWritable.setETag(currentKey.getETag());
		s3ObjectSummaryWritable.setLastModified(currentKey.getLastModified());
		s3ObjectSummaryWritable.setOwner(currentKey.getOwner());
		s3ObjectSummaryWritable.setSize(currentKey.getSize());
		s3ObjectSummaryWritable.setStorageClass(currentKey.getStorageClass());

		object = reader.getObject(currentKey);
		s3ObjectWritable.setBucketName(object.getBucketName());
		s3ObjectWritable.setKey(object.getKey());
		s3ObjectWritable.setObjectContent(object.getObjectContent());
		s3ObjectWritable.setObjectMetadata(object.getObjectMetadata());
	}

	public S3ObjectSummaryWritable createKey() {
		return new S3ObjectSummaryWritable();
	}

	public S3ObjectWritable createValue() {
		return new S3ObjectWritable();
	}
}
