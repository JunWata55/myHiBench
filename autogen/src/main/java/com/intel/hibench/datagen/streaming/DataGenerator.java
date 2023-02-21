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

package com.intel.hibench.datagen.streaming;

import com.intel.hibench.common.HiBenchConfig;
import com.intel.hibench.common.streaming.ConfigLoader;
import com.intel.hibench.common.streaming.StreamBenchConfig;
import com.intel.hibench.datagen.streaming.util.DataGeneratorConfig;
import com.intel.hibench.datagen.streaming.util.KafkaSender;
import com.intel.hibench.datagen.streaming.util.RecordSendTask;

import java.util.Timer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// when you want to change the rate
// /home/junwata/HiBench/report/wordcount/prepare/conf/sparkbench/sparkbench.conf
//    = StreamBenchConfig.DATAGEN_INTERVAL_SPAN
//    = StreamBenchConfig.DATAGEN_RECORDS_PRE_INTERVAL
//        -> timer.scheduleAtFixedRate()

// when you want to change the data set size
// 1.
// -> com.intel.hibench.datagen.streaming.util.KafkaSender
//    -> com.intel.hibench.datagen.streaming.util.CachedData
//       -> change the field value, totalRecord (because of the round robin)

public class DataGenerator {

  public static void main(String[] args) {
    if (args.length < 7) {
      System.err.println("args: <ConfigFile> <userVisitsFile> <userVisitsFileOffset> <kMeansFile> <kMeansFileOffset> <dataSetSize> <rateChanger> need to be specified!");
      System.exit(1);
    }

    // initialize variable from configuration and input parameters.
    ConfigLoader configLoader = new ConfigLoader(args[0]); // /home/junwata/HiBench/report/wordcount/prepare/conf/sparkbench/sparkbench.conf

    String userVisitsFile = args[1];
    long userVisitsFileOffset = Long.parseLong(args[2]); // hdfs://localhost:9000/HiBench/Streaming/Seed/uservisits
    String kMeansFile = args[3];
    long kMeansFileOffset = Long.parseLong(args[4]); // hdfs://localhost:9000/HiBench/Streaming/Kmeans/Samples

    int dataSize = Integer.parseInt(args[5]);
    int multiRecord = Integer.parseInt(args[6]);
    int multiInterval = Integer.parseInt(args[7]);
    System.out.println("The user defined parameters:");
    System.out.println("\tData set size: " + dataSize);
    System.out.println("\tNumber to multiple with the number of records per interval: " + multiRecord);
    System.out.println("\tNumber to multiple with the length of interval: " + multiInterval);

    // load properties from config file
    String testCase = configLoader.getProperty(StreamBenchConfig.TESTCASE).toLowerCase(); // wordcount
    String topic = configLoader.getProperty(StreamBenchConfig.KAFKA_TOPIC); // wordcount
    String brokerList = configLoader.getProperty(StreamBenchConfig.KAFKA_BROKER_LIST); // 127.0.0.1:9092
    int intervalSpan = Integer.parseInt(configLoader.getProperty(StreamBenchConfig.DATAGEN_INTERVAL_SPAN)); // 50 ms
    intervalSpan *= multiInterval;
    long recordsPerInterval = Long.parseLong(configLoader.getProperty(StreamBenchConfig.DATAGEN_RECORDS_PRE_INTERVAL)); // 5
    recordsPerInterval *= multiRecord;
    long totalRecords = Long.parseLong(configLoader.getProperty(StreamBenchConfig.DATAGEN_TOTAL_RECORDS)); // -1
    int totalRounds = Integer.parseInt(configLoader.getProperty(StreamBenchConfig.DATAGEN_TOTAL_ROUNDS)); // -1
    int recordLength = Integer.parseInt(configLoader.getProperty(StreamBenchConfig.DATAGEN_RECORD_LENGTH)); // 200 bytes
    String dfsMaster = configLoader.getProperty(HiBenchConfig.DFS_MASTER); // hdfs://localhost:9000
    boolean debugMode = Boolean.getBoolean(configLoader.getProperty(StreamBenchConfig.DEBUG_MODE)); // false
    System.out.println("The result of inputs:");
    System.out.println("\tthe number of records per interval: " + recordsPerInterval);
    System.out.println("\tthe length of interval: " + intervalSpan);


    // DataGeneratorConfig dataGeneratorConf = new DataGeneratorConfig(testCase, brokerList, kMeansFile, kMeansFileOffset,
    //     userVisitsFile, userVisitsFileOffset, dfsMaster, recordLength, intervalSpan, topic, recordsPerInterval,
    //     totalRounds, totalRecords, debugMode);

    DataGeneratorConfig dataGeneratorConf = new DataGeneratorConfig(testCase, brokerList, kMeansFile, kMeansFileOffset,
        userVisitsFile, userVisitsFileOffset, dfsMaster, recordLength, intervalSpan, topic, recordsPerInterval,
        totalRounds, totalRecords, debugMode, dataSize);

    // Create thread pool and submit producer task
    int producerNumber = Integer.parseInt(configLoader.getProperty(StreamBenchConfig.DATAGEN_PRODUCER_NUMBER)); // 1
    ExecutorService pool = Executors.newFixedThreadPool(producerNumber);
    for(int i = 0; i < producerNumber; i++) {
      pool.execute(new DataGeneratorJob(dataGeneratorConf));
    }

    // Print out some basic information
    System.out.println("============ StreamBench Data Generator ============");
    System.out.println(" Interval Span       : " + intervalSpan + " ms");
    System.out.println(" Record Per Interval : " + recordsPerInterval);
    System.out.println(" Record Length       : " + recordLength + " bytes");
    System.out.println(" Producer Number     : " + producerNumber);
    if(totalRecords == -1) {
      System.out.println(" Total Records        : -1 [Infinity]");
    } else {
      System.out.println(" Total Records        : " + totalRecords);
    }

    if (totalRounds == -1) {
      System.out.println(" Total Rounds         : -1 [Infinity]");
    } else {
      System.out.println(" Total Rounds         : " + totalRounds);
    }
    System.out.println(" Kafka Topic          : " + topic);
    System.out.println("====================================================");
    System.out.println("Estimated Speed : ");
    long recordsPreSecond = recordsPerInterval * 1000 * producerNumber / intervalSpan ;
    System.out.println("    " + recordsPreSecond + " records/second");
    double mbPreSecond = (double)recordsPreSecond * recordLength / 1000000;
    System.out.println("    " + mbPreSecond + " Mb/second");
    System.out.println("====================================================");

    pool.shutdown();
  }

  static class DataGeneratorJob implements Runnable {
    DataGeneratorConfig conf;

    // Constructor
    public DataGeneratorJob(DataGeneratorConfig conf) {
      this.conf = conf;
    }

    @Override
    public void run() {
      // instantiate KafkaSender
      KafkaSender sender;
      if(conf.getTestCase().contains("statistics")) {
        sender = new KafkaSender(conf.getBrokerList(), conf.getkMeansFile(), conf.getkMeansFileOffset(),
            conf.getDfsMaster(), conf.getRecordLength(), conf.getIntervalSpan(), conf.getDataSize());
      } else {
        sender = new KafkaSender(conf.getBrokerList(), conf.getUserVisitsFile(), conf.getUserVisitsFileOffset(),
            conf.getDfsMaster(), conf.getRecordLength(), conf.getIntervalSpan(), conf.getDataSize());
      }
      // if(conf.getTestCase().contains("statistics")) {
      //   sender = new KafkaSender(conf.getBrokerList(), conf.getkMeansFile(), conf.getkMeansFileOffset(),
      //       conf.getDfsMaster(), conf.getRecordLength(), conf.getIntervalSpan());
      // } else {
      //   sender = new KafkaSender(conf.getBrokerList(), conf.getUserVisitsFile(), conf.getUserVisitsFileOffset(),
      //       conf.getDfsMaster(), conf.getRecordLength(), conf.getIntervalSpan());
      // }

      // Schedule timer task
      Timer timer = new Timer();
      timer.scheduleAtFixedRate(
          new RecordSendTask(sender, conf.getTopic(), conf.getRecordsPerInterval(),
              conf.getTotalRounds(), conf.getTotalRecords(), conf.getDebugMode(), timer), 0, conf.getIntervalSpan());
    }
  }
}
