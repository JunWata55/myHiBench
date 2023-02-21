/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.intel.hibench.datagen.streaming.util;

import org.apache.hadoop.conf.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Cache the total records in memory.
 */
public class CachedData {

  private volatile static CachedData cachedData;

  private List<String> data;

  private int next;
  private int totalRecords;

  public static CachedData getInstance(String seedFile, long fileOffset, String dfsMaster, int totalRecords) {
    if(cachedData == null) {
      synchronized (CachedData.class) {
        if (cachedData == null) {
          cachedData = new CachedData(seedFile, fileOffset, dfsMaster, totalRecords);
        }
      }
    }
    return cachedData;
  }

  private CachedData(String seedFile, long fileOffset, String dfsMaster, int totalRecords){
  // private CachedData(String seedFile, long fileOffset, String dfsMaster){
      Configuration dfsConf = new Configuration();
    // dfsConf.set("fs.default.name", dfsMaster); // hdfs://localhost:9000
    dfsConf.set("fs.defaultFS", dfsMaster);

    // read records from seedFile and cache into "data"
    data = new ArrayList<String>();
    BufferedReader reader = SourceFileReader.getReader(dfsConf, seedFile, fileOffset);
    String line = null;
    try {
      while ((line = reader.readLine()) != null) {
        data.add(line); // the default num of dataset: 1000000
        // sample
        // 136     216.56.204.14,tqiokmubgzkkjzhgvczyuzaxploykpodcwsbhdkgwzcvllvvcbysokkocjnkijqozvb    gssvnxzwddecxwjsplexlghyu,1972-11-24,0.31341314,Mozilla/5.0 (Windows; U; Windows NT 5.2)A    ppleWebKit/525.13 (KHTML like Gecko) Version/3.1Safari/525.13,MNE,MNE-SR,bigamist's,9
      }
    } catch (IOException e) {
      System.err.println("Failed read records from Path: " + seedFile);
      e.printStackTrace();
    }

    this.next = 0;
    // we can change the data set size by decrease the total records(must be less than the actual total records)
    // this.totalRecords = data.size();
    // this.totalRecords = 1000;
    if (totalRecords < data.size()) {
      this.totalRecords = totalRecords;
    } else  {
      this.totalRecords = data.size();
    }
    System.out.println("the number of the total records are " + this.totalRecords);
  }

  /**
   * Loop get record.
   * @return the record.
   */
  // round robin I guess
  public String getRecord() {
    int current = next;
    next = (next + 1) % totalRecords;
    return data.get(current);
  }
}
