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

import org.apache.nifi.util.lookup.io.IPv4CIDRBlock;
import org.apache.nifi.util.lookup.io.RecordReader;

public class BulkLoadedIPv4PrefixMatchOffHeapLookup {	
	private volatile long currentInstance = 0;
	private final ReadWriteLock lock = new ReentrantReadWriteLock();
	private final Lock writeLock = lock.writeLock();
	private final Lock readLock = lock.readLock();

	public byte [] lookup( int ip ){
		byte [] rv = null;
		readLock.lock();
		try{
			if(currentInstance != 0 ){
				rv = OffHeapLookup.trieLookup(currentInstance, ip);
			}
		}
		finally{
			readLock.unlock();
		}
		return rv;
	}
	
	
	public void loadThenSwap(final InputStream in, final RecordReader<IPv4CIDRBlock, byte[]> callback){
		// TODO throw exception on bad size
		long newInstance = OffHeapLookup.newTrie();
		try{
			callback.initialize(in);
			while(callback.nextKeyValue() == true){
				final IPv4CIDRBlock key = callback.getCurrentKey();
				final byte [] value = callback.getCurrentValue();
				// TODO: null check key and value 
				// TODO: catch errors from insert
				OffHeapLookup.trieInsert(newInstance, key.getStartAddress(), key.getPrefixLength(), value);
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
			OffHeapLookup.deleteTrie(temp);	
		}
		
	}

	
	public void tearDown(){
		long temp = currentInstance;
		writeLock.lock();		
		currentInstance = 0;
		writeLock.unlock();
		if(temp != 0){
			OffHeapLookup.deleteTrie(temp);	
		}
		
	}
}
