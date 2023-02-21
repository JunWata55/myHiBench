// changed the class name: the name KafkaConsumer already exists in imported libray

package com.intel.hibench.common.streaming.metrics

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import java.util.Arrays
import java.util.List
import org.apache.kafka.clients.consumer.ConsumerRecord
import java.time.Duration
import java.util.Iterator

class KafkaUser(bootstrapConnect: String, topic: String, partition: Int) {
    private val CLIENT_ID = "metrics_reader"
    private val props = new Properties()
    props.setProperty("bootstrap.servers", bootstrapConnect)
    props.setProperty("group.id", CLIENT_ID)
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    private val consumer = createConsumer
    private var iterator: Iterator[ConsumerRecord[String, String]] = getIterator

    def next(): String = {
        val record = iterator.next()
        record.value()
    }
    
    def hasNext: Boolean = {
        @annotation.tailrec
        def hasNextHelper(iter: Iterator[ConsumerRecord[String, String]], newIterator: Boolean): Boolean = {
            if (iter.hasNext) true
            else if (newIterator) false
            else {
                iterator = getIterator
                hasNextHelper(iterator, newIterator = true)
            }
        }
        hasNextHelper(iterator, newIterator = false)
    }

    private def createConsumer: KafkaConsumer[String, String] = {
        val consumerBase = new KafkaConsumer[String, String](props)
        val topicPartition = new TopicPartition(topic, partition)
        val partitions = Arrays.asList(topicPartition)
        consumerBase.assign(partitions)
        consumerBase.seekToBeginning(partitions)
        consumerBase
    }

    private def getIterator(): Iterator[ConsumerRecord[String, String]] = {
        return consumer.poll(Duration.ofMillis(100)).iterator()
    }
}

// /*
//  * Licensed to the Apache Software Foundation (ASF) under one or more
//  * contributor license agreements.  See the NOTICE file distributed with
//  * this work for additional information regarding copyright ownership.
//  * The ASF licenses this file to You under the Apache License, Version 2.0
//  * (the "License"); you may not use this file except in compliance with
//  * the License.  You may obtain a copy of the License at
//  *
//  *    http://www.apache.org/licenses/LICENSE-2.0
//  *
//  * Unless required by applicable law or agreed to in writing, software
//  * distributed under the License is distributed on an "AS IS" BASIS,
//  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  * See the License for the specific language governing permissions and
//  * limitations under the License.
//  */
// package com.intel.hibench.common.streaming.metrics

// import java.util.Properties

// import kafka.api.{OffsetRequest, FetchRequestBuilder}
// import kafka.common.ErrorMapping._
// import kafka.common.TopicAndPartition
// import kafka.consumer.{ConsumerConfig, SimpleConsumer}
// import kafka.message.MessageAndOffset
// import kafka.utils.{ZKStringSerializer, ZkUtils, Utils}
// import org.I0Itec.zkclient.ZkClient

// class KafkaConsumer(zookeeperConnect: String, topic: String, partition: Int) {

//   private val CLIENT_ID = "metrics_reader"
//   private val props = new Properties()
//   props.put("zookeeper.connect", zookeeperConnect)
//   props.put("group.id", CLIENT_ID)
//   private val config = new ConsumerConfig(props)
//   private val consumer = createConsumer

//   private val earliestOffset = consumer
//       .earliestOrLatestOffset(TopicAndPartition(topic, partition), OffsetRequest.EarliestTime, -1)
//   private var nextOffset: Long = earliestOffset
//   private var iterator: Iterator[MessageAndOffset] = getIterator(nextOffset)

//   def next(): Array[Byte] = {
//     val mo = iterator.next()
//     val message = mo.message

//     nextOffset = mo.nextOffset

//     Utils.readBytes(message.payload)
//   }

//   def hasNext: Boolean = {
//     @annotation.tailrec
//     def hasNextHelper(iter: Iterator[MessageAndOffset], newIterator: Boolean): Boolean = {
//       if (iter.hasNext) true
//       else if (newIterator) false
//       else {
//         iterator = getIterator(nextOffset)
//         hasNextHelper(iterator, newIterator = true)
//       }
//     }
//     hasNextHelper(iterator, newIterator = false)
//   }

//   def close(): Unit = {
//     consumer.close()
//   }

//   private def createConsumer: SimpleConsumer = {
//     val zkClient = new ZkClient(zookeeperConnect, 6000, 6000, ZKStringSerializer)
//     try {
//       val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)
//           .getOrElse(throw new RuntimeException(
//             s"leader not available for TopicAndPartition($topic, $partition)"))
//       val broker = ZkUtils.getBrokerInfo(zkClient, leader)
//           .getOrElse(throw new RuntimeException(s"broker info not found for leader $leader"))
//       new SimpleConsumer(broker.host, broker.port,
//         config.socketTimeoutMs, config.socketReceiveBufferBytes, CLIENT_ID)
//     } catch {
//       case e: Exception =>
//         throw e
//     } finally {
//       zkClient.close()
//     }
//   }

//   private def getIterator(offset: Long): Iterator[MessageAndOffset] = {
//     val request = new FetchRequestBuilder()
//         .addFetch(topic, partition, offset, config.fetchMessageMaxBytes)
//         .build()

//     val response = consumer.fetch(request)
//     response.errorCode(topic, partition) match {
//       case NoError => response.messageSet(topic, partition).iterator
//       case error => throw exceptionFor(error)
//     }
//   }
// }
