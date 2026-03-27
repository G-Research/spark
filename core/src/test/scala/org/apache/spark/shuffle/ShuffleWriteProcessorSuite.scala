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
package org.apache.spark.shuffle

import org.mockito.ArgumentMatchers.{any, eq => meq}
import org.mockito.Mockito.when
import org.scalatestplus.mockito.MockitoSugar.mock

import org.apache.spark.{HashPartitioner, Partition, ShuffleDependency, SparkConf, SparkContext, SparkEnv, SparkFunSuite, TaskContextImpl}
import org.apache.spark.internal.config.{STORAGE_DECOMMISSION_FALLBACK_STORAGE_ALWAYS_READ, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_ENABLED, STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_RELIABLE}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.MapStatus
import org.apache.spark.shuffle.api.ShuffleDriverComponents
import org.apache.spark.storage.{BlockManager, BlockManagerId, BlockManagerMaster, DiskBlockManager, FallbackStorage}

class ShuffleWriteProcessorSuite extends SparkFunSuite {

  test("write returns map status given by writer.stop") {
    doTest()
  }

  test("write returns map status with fallback storage location") {
    val conf = new SparkConf(false)
    conf.set("spark.app.id", "testing")
    conf.set(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PATH, "/tmp/")
    conf.set(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_ENABLED, true)
    conf.set(STORAGE_DECOMMISSION_FALLBACK_STORAGE_PROACTIVE_RELIABLE, true)
    conf.set(STORAGE_DECOMMISSION_FALLBACK_STORAGE_ALWAYS_READ, true)
    doTest(Some(conf), Some(FallbackStorage.FALLBACK_BLOCK_MANAGER_ID))
  }

  def doTest(
      confOpt: Option[SparkConf] = None,
      expectedMapStatusLocation: Option[BlockManagerId] = None): Unit = {
    val conf = confOpt.getOrElse(new SparkConf(false))

    val context = new TaskContextImpl(1, 1, 0, 1, 1, 2, null, null, null, cpus = 1)
    val mapId = 1L

    val bmId = BlockManagerId("exec-1", "host", 1234)
    val mapStatus = MapStatus(bmId, Array(10L, 20L), mapId, 0L)

    val writer = mock[ShuffleWriter[Int, Int]]
    when(writer.stop(true)).thenReturn(Some(mapStatus))

    val shuffleManager = mock[ShuffleManager]
    when(shuffleManager.getWriter[Int, Int](any(), meq(mapId), meq(context), any()))
      .thenReturn(writer)

    val blockManager = mock[BlockManager]
    val dbm = new DiskBlockManager(conf, deleteFilesOnStop = false, isDriver = false)
    val bmm = mock[BlockManagerMaster]
    when(blockManager.diskBlockManager).thenReturn(dbm)
    when(blockManager.master).thenReturn(bmm)
    val resolver = new IndexShuffleBlockResolver(conf, blockManager)
    when(blockManager.migratableResolver).thenReturn(resolver)

    val env = mock[SparkEnv]
    SparkEnv.set(env)
    when(env.conf).thenReturn(conf)
    when(env.shuffleManager).thenReturn(shuffleManager)
    when(env.blockManager).thenReturn(blockManager)

    val sc = mock[SparkContext]
    when(sc.env).thenReturn(env)
    when(sc.conf).thenReturn(conf)
    when(sc.newShuffleId()).thenReturn(1)
    when(sc.cleaner).thenReturn(None)
    when(sc.shuffleDriverComponents).thenReturn(mock[ShuffleDriverComponents])

    val partitions = (0 to 1).toArray.map(id => new Partition { override def index: Int = id })
    val rdd = mock[RDD[Product2[Int, Int]]]
    when(rdd.context).thenReturn(sc)
    when(rdd.sparkContext).thenReturn(sc)
    when(rdd.partitions).thenReturn(partitions)

    val it = Iterator.empty
    val dep = new ShuffleDependency[Int, Int, Int](rdd, new HashPartitioner(2), null,
      shuffleWriterProcessor = new ShuffleWriteProcessor())
    val actualMapStatus = dep.shuffleWriterProcessor.write(it, dep, 1L, 0, context)
    assert(actualMapStatus.location === expectedMapStatusLocation.getOrElse(bmId))
  }

}
