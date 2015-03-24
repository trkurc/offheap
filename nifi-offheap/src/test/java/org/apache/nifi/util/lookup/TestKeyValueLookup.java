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

import java.io.ByteArrayInputStream;

import org.apache.nifi.util.lookup.io.CSVRecordReader;
import org.junit.Test;
import static org.junit.Assert.*;
public class TestKeyValueLookup {
	@Test
	public void testBulkLoadedKeyValue(){
		BulkLoadedKeyValueOffHeapLookup x = new BulkLoadedKeyValueOffHeapLookup();
		String text = "AAAAAAAA,111111111\n" +
				      "BBBBBBBB,222222222\n" +
				      "CCCCCCCC,333333333\n" +
				      "DDDDDDDD,444444444\n" +
				      "EEEEEEEE,555555555";
		ByteArrayInputStream bis = new ByteArrayInputStream(text.getBytes());
		
		x.loadThenSwap(bis, new CSVRecordReader());
		
		assertArrayEquals(x.get("AAAAAAAA".getBytes()),  "111111111".getBytes());
		assertArrayEquals(x.get("BBBBBBBB".getBytes()),  "222222222".getBytes());
		assertArrayEquals(x.get("CCCCCCCC".getBytes()),  "333333333".getBytes());
		assertArrayEquals(x.get("DDDDDDDD".getBytes()),  "444444444".getBytes());
		assertArrayEquals(x.get("EEEEEEEE".getBytes()),  "555555555".getBytes());
		assertNull(x.get("FFFFFFFF".getBytes()));
		x.tearDown();
	}
	
}
