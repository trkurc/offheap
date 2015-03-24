/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements. See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package org.apache.nifi.util.lookup;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.nifi.util.lookup.io.RecordReader;

public class BulkLoadedKeyValueOffHeapLookup {
	private static final int DEFAULT_HTABLE_SIZE = 1048573; // prime number close to 1<<20
	
	private volatile long currentInstance = 0;
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock writeLock = lock.writeLock();
	private final Lock readLock = lock.readLock();

	public byte [] get( final byte [] key ){
		byte [] rv = null;
		readLock.lock();
		try{
			if(currentInstance != 0 && key != null){
				rv = OffHeapLookup.htableLookup(currentInstance, key);
			}
		}
		finally{
			readLock.unlock();
		}
		return rv;
	}
	
	
	public void loadThenSwap(final InputStream in, final RecordReader<byte [], byte[]> callback, int size){
		// TODO throw exception on bad size
		long newInstance = OffHeapLookup.newHtable(size);
		try{
			callback.initialize(in);
			while(callback.nextKeyValue() == true){
				final byte [] key = callback.getCurrentKey();
				final byte [] value = callback.getCurrentValue();
				// TODO: null check key and value
				// TODO: catch errors from insert
				OffHeapLookup.htableInsert(newInstance, key, value);
			}		
			callback.close();
		}
		catch(InterruptedException e){
			throw new RuntimeException("!!");
		}
		catch(IOException e){
			throw new RuntimeException("!!");
		}
		
		long temp = currentInstance;
		writeLock.lock();		
		currentInstance = newInstance;
		writeLock.unlock();
		
		if(temp != 0){
			OffHeapLookup.deleteHtable(temp);	
		}
		
	}
	public void loadThenSwap(final InputStream in, final RecordReader<byte [], byte[]> callback){
		loadThenSwap(in, callback, DEFAULT_HTABLE_SIZE);
	}
	

	
	public void tearDown(){
		long temp = currentInstance;
		writeLock.lock();		
		currentInstance = 0;
		writeLock.unlock();
		if(temp != 0){
			OffHeapLookup.deleteHtable(temp);	
		}
		
	}
}
