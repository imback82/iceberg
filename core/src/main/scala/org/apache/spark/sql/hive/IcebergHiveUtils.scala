/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.spark.sql.hive

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.hive.client.HiveClient

object IcebergHiveUtils extends org.apache.spark.internal.Logging {
  //  def getClassLoader(): ClassLoader = {
  //    null
  //    //    val session = SparkSession.active
  //    //    val sparkConf = session.sharedState.conf
  //    //    val hadoopConf = session.sharedState.hadoopConf
  //    //    val client = HiveUtils.newClientForMetadata(sparkConf, hadoopConf)
  //    //    logInfo("Iceberg!!!: " + client.listTables("default").mkString(","))
  //    //    HiveUtils.isolatedClientLoader.classLoader
  //  }

  def getClassLoader(conf: SparkConf, hadoopConf: Configuration): ClassLoader = {
    HiveUtils.initClassLoader(conf, hadoopConf)
    HiveUtils.isolatedClientLoader.classLoader
  }

  def newClientForMetadata(conf: SparkConf, hadoopConf: Configuration): HiveClient = {
    HiveUtils.newClientForMetadata(conf, hadoopConf)
  }
}
