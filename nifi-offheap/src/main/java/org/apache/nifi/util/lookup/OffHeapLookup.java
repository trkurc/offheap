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
	
	static native long newTrie();
	static native void deleteTrie(long instance);
	static native byte [] trieLookup(long instance, int address);
	static native void trieInsert(long instance, int address, int mask, byte [] value );

	/* Key/Value Lookup Methods */
	
	static native long newHtable(int size);
	static native void deleteHtable(long instance);
	static native byte [] htableLookup(long instance, byte [] key);
	static native void htableInsert(long instance, byte [] key, byte [] value );
	
	static {
		// TODO: Load this in a sensible way
		System.loadLibrary("offheap");
	}
}
