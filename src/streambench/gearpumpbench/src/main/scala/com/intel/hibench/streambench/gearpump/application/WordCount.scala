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

package com.intel.hibench.streambench.gearpump.application

import com.intel.hibench.streambench.common.TestCase
import com.intel.hibench.streambench.gearpump.source.SourceProvider
import com.intel.hibench.streambench.gearpump.task.{Parser, Sum}
import com.intel.hibench.streambench.gearpump.util.GearpumpConfig
import org.apache.gearpump.cluster.UserConfig
import org.apache.gearpump.partitioner.ShufflePartitioner
import org.apache.gearpump.streaming.{Processor, StreamApplication}
import org.apache.gearpump.util.Graph
import org.apache.gearpump.util.Graph._

class WordCount(conf: GearpumpConfig)(implicit sourceProvider: SourceProvider) extends BasicApplication(conf) {
  override val benchName = TestCase.WORDCOUNT

  override def application(benchConfig: UserConfig): StreamApplication = {
    val source = getSource()
    val partitioner = new ShufflePartitioner
    val parser = Processor[Parser](conf.parallelism)
    val sum = Processor[Sum](conf.parallelism)
    StreamApplication("wordCount", Graph(source ~ partitioner ~> parser ~> sum), benchConfig)
  }
}