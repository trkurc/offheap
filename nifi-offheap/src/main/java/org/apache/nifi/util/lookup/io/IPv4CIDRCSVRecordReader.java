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
package org.apache.nifi.util.lookup.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;





public class IPv4CIDRCSVRecordReader implements RecordReader<IPv4CIDRBlock, byte[]> {
	private BufferedReader br;
	private IPv4CIDRBlock key = new IPv4CIDRBlock();
	private byte [] value;
	
	public boolean nextKeyValue() throws IOException{
		String line = br.readLine();
		if(line == null) return false;
		String [] pieces = line.split(",");
		if(pieces.length != 2){
			return false;
		}
		
		String [] cidr= pieces[0].split("/");
		if(cidr.length != 2){
			return false;
		}
		int ip = ipAsInt(cidr[0]);
		short prefixLength = Short.valueOf(cidr[1]);
		key.set(ip, prefixLength);
		
		value = pieces[1].getBytes();
		return true;
	}
	public IPv4CIDRBlock getCurrentKey(){
		return key;
	}
	
	public byte [] getCurrentValue(){
		return value;
	}
	
	public void initialize(InputStream input){
		br = new BufferedReader(new InputStreamReader(input));
	}
	public void close(){
		try {
			br.close();
		} catch (IOException e) {
			// Swallow
		}
	}
	
	public static int ipAsInt(String ip) {
	    String[] octets = ip.split("\\.");
	    if(octets.length != 4) throw new RuntimeException("Unable to convert ip address");
	    int shift = 24;
	    int rv = 0;
	    for(int i=0; i < 4; i++){
	    	rv |= (Integer.parseInt(octets[i]) << shift);
	    	shift -= 8;
	    }
	    return rv;
	}
	
}
