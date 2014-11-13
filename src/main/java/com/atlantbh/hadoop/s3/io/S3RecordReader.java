package com.atlantbh.hadoop.s3.io;

import java.io.IOException;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.s3.model.S3ObjectSummary;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Abstract S3 record reader class
 * 
 * @author seljaz
 * @param <KEY>
 * @param <VALUE>
 *  
 */
public abstract class S3RecordReader<KEY, VALUE> implements RecordReader<KEY, VALUE> {

	Logger LOG = LoggerFactory.getLogger(S3RecordReader.class);

	String bucketName;
	String keyPrefix;
	String marker;
	String lastKey;
	
	int size;
	int currentPosition = 0;
	int maxKeys;

	S3BucketReader reader = null;

	S3ObjectSummary currentKey;

	public void initialize(InputSplit split, JobConf job) throws IOException, InterruptedException {

		S3InputSplit inputSplit = (S3InputSplit) split;

		bucketName = inputSplit.getBucketName();
		keyPrefix = inputSplit.getKeyPrefix();
		marker = inputSplit.getMarker();
		lastKey = inputSplit.lastKey;
		
		maxKeys = job.getInt(S3InputFormat.S3_MAX_KEYS, 100);

		reader = new S3BucketReader(bucketName, keyPrefix, marker, maxKeys);
	}

	public boolean next(KEY key, VALUE value) throws IOException {
		while ((currentKey = reader.getNextKey()) != null) {
			// have we reached end of the split
			if (currentKey.getKey().compareTo(lastKey) <= 0) {
				currentPosition++;
				setNextKeyValue(key, value);
				return true;
			} else {
				return false;
			}
		}
		return false;
	}

	// Subclasses must override this
	protected void setNextKeyValue(KEY key, VALUE value) {
		throw new NotImplementedException();
	}

	public long getPos() {
		return currentPosition;
	}

	public float getProgress() {
		return currentPosition/(float)size;
	}

	public void close() throws IOException {
	}
}