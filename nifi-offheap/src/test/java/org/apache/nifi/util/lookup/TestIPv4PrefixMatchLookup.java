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

import static org.apache.nifi.util.lookup.io.IPv4CIDRCSVRecordReader.ipAsInt;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;

import org.apache.nifi.util.lookup.io.IPv4CIDRCSVRecordReader;
import org.junit.Test;
public class TestIPv4PrefixMatchLookup {
	@Test
	public void testBulkLoadedIPv4PrefixMatch(){
		BulkLoadedIPv4PrefixMatchOffHeapLookup x = new BulkLoadedIPv4PrefixMatchOffHeapLookup();
		String text = "10.10.10.0/24,111111111\n" +
				      "10.10.10.10/32,222222222\n" +
				      "10.10.0.0/16,333333333\n" +
				      "127.0.0.0/8,444444444\n" +
				      "255.6.7.0/26,555555555";
		ByteArrayInputStream bis = new ByteArrayInputStream(text.getBytes());
		
		x.loadThenSwap(bis, new IPv4CIDRCSVRecordReader());
		
		assertArrayEquals(x.lookup(ipAsInt("10.10.10.1")),  "111111111".getBytes());
		assertArrayEquals(x.lookup(ipAsInt("10.10.10.10")),  "222222222".getBytes());
		assertArrayEquals(x.lookup(ipAsInt("10.10.255.1")),  "333333333".getBytes());
		assertArrayEquals(x.lookup(ipAsInt("127.255.0.127")),  "444444444".getBytes());
		assertArrayEquals(x.lookup(ipAsInt("255.6.7.63")),  "555555555".getBytes());
		assertNull(x.lookup(ipAsInt("255.6.7.64")));
		x.tearDown();
	}
	
}
