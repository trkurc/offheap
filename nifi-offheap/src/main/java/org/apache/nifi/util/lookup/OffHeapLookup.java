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

public class OffHeapLookup {

	/* 32 Bit Longest Prefix Match Methods */
	
	private static native long newTrie();
	private static native void deleteTrie(long instance);
	private static native byte [] trieLookup(long instance, int address);
	private static native void trieInsert(long instance, int address, int mask, byte [] value );

	/* Key/Value Lookup Methods */
	
	private static native long newHtable(int size);
	private static native void deleteHtable(long instance);
	private static native byte [] htableLookup(long instance, byte [] key);
	private static native void htableInsert(long instance, byte [] key, byte [] value );
	
	public static void main(String args[]){
		System.loadLibrary("offheap");

		long instance = newHtable(17);
		htableInsert(instance, "monkey".getBytes(), "hello".getBytes());
		System.out.println(new String(htableLookup(instance, "monkey2".getBytes())));
		
	}
	
}
