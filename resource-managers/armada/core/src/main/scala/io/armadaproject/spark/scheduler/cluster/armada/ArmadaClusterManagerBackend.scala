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
package org.apache.spark.scheduler.cluster.armada

import scala.collection.mutable.HashMap

import java.util.concurrent.{ScheduledExecutorService} //, TimeUnit}
import org.apache.spark.scheduler.{ExecutorDecommission, TaskSchedulerImpl} //, ExecutorDecommissionInfo, ExecutorKilled, ExecutorLossReason,
  //TaskSchedulerImpl}
import org.apache.spark.rpc.{RpcAddress, RpcCallContext}
import org.apache.spark.SparkContext
import org.apache.spark.scheduler.cluster.{CoarseGrainedSchedulerBackend, SchedulerBackendUtils}
//import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.RegisterExecutor

// FIXME: Actually import ArmadaClient
class ArmadaClient {}

// TODO: Implement for Armada
private[spark] class ArmadaClusterSchedulerBackend(
    scheduler: TaskSchedulerImpl,
    sc: SparkContext,
    armadaClient: ArmadaClient,
    executorService: ScheduledExecutorService)
    extends CoarseGrainedSchedulerBackend(scheduler, sc.env.rpcEnv) {

    // FIXME
    private val appId = "fake_app_id_FIXME"

    private val initialExecutors = SchedulerBackendUtils.getInitialTargetExecutorNumber(conf)

    override def applicationId(): String = {
        conf.getOption("spark.app.id").getOrElse(appId)
    }

    override def start(): Unit = {}
    override def stop(): Unit = {}

    /*
    override def doRequestTotalExecutors(
      resourceProfileToTotalExecs: Map[ResourceProfile, Int]): Future[Boolean] = {
        //podAllocator.setTotalExpectedExecutors(resourceProfileToTotalExecs)
        //Future.successful(true)
    }*/

    override def sufficientResourcesRegistered(): Boolean = {
      totalRegisteredExecutors.get() >= initialExecutors * minRegisteredRatio
    }

    override def getExecutorIds(): Seq[String] = synchronized {
      super.getExecutorIds()
    }
    
    override def createDriverEndpoint(): DriverEndpoint = {
      new ArmadaDriverEndpoint()
    }

    private class ArmadaDriverEndpoint extends DriverEndpoint {
      protected val execIDRequester = new HashMap[RpcAddress, String]

      override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] =
            super.receiveAndReply(context)
        /*generateExecID(context).orElse(
          ignoreRegisterExecutorAtStoppedContext.orElse(
            super.receiveAndReply(context)))*/

      override def onDisconnected(rpcAddress: RpcAddress): Unit = {
        val execId = addressToExecutorId.get(rpcAddress)
        execId match {
          case Some(id) =>
            executorsPendingDecommission.get(id) match {
              case Some(host) =>
                // We don't pass through the host because by convention the
                // host is only populated if the entire host is going away
                // and we don't know if that's the case or just one container.
                removeExecutor(id, ExecutorDecommission(None))
              case _ =>
                // Don't do anything besides disabling the executor - allow the K8s API events to
                // drive the rest of the lifecycle decisions.
                // If it's disconnected due to network issues eventually heartbeat will clear it up.
                disableExecutor(id)
            }
          case _ =>
            val newExecId = execIDRequester.get(rpcAddress)
            newExecId match {
              case Some(id) =>
                execIDRequester -= rpcAddress
                // Expected, executors re-establish a connection with an ID
              case _ =>
                logDebug(s"No executor found for ${rpcAddress}")
            }
        }
    }
  }
}
