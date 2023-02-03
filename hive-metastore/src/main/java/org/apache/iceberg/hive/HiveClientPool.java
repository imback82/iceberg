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
package org.apache.iceberg.hive;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.ClientPoolImpl;
import org.apache.iceberg.relocated.com.google.common.annotations.VisibleForTesting;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.hive.IcebergHiveUtils;
import org.apache.spark.sql.hive.client.HiveClient;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.immutable.Set;

public class HiveClientPool extends ClientPoolImpl<HiveClient, TException> {
  private static final Logger LOG = LoggerFactory.getLogger(BaseMetastoreCatalog.class);

  //  private static final DynMethods.StaticMethod GET_CLIENT =
  //      DynMethods.builder("getProxy")
  //          // .loader(IcebergHiveUtils.getClassLoader())
  //          .impl(
  //              RetryingMetaStoreClient.class,
  //              HiveConf.class,
  //              HiveMetaHookLoader.class,
  //              String.class) // Hive 1 and 2
  //          .impl(
  //              RetryingMetaStoreClient.class,
  //              Configuration.class,
  //              HiveMetaHookLoader.class,
  //              String.class) // Hive 3
  //          .buildStatic();

  private final HiveConf hiveConf;

  public HiveClientPool(int poolSize, Configuration conf) {
    // Do not allow retry by default as we rely on RetryingHiveClient
    super(poolSize, TTransportException.class, false);
    this.hiveConf = new HiveConf(conf, HiveClientPool.class);
    this.hiveConf.addResource(conf);
  }

  @Override
  protected HiveClient newClient() {
    SparkSession session = SparkSession.active();
    SparkConf sparkConf = session.sharedState().conf();
    Configuration hadoopConf = session.sharedState().hadoopConf();
    // ClassLoader original = Thread.currentThread().getContextClassLoader();
    // ClassLoader loader = IcebergHiveUtils.getClassLoader(sparkConf, hadoopConf);
    // Thread.currentThread().setContextClassLoader(loader);
    HiveClient hc = IcebergHiveUtils.newClientForMetadata(sparkConf, hadoopConf);
    // IMetaStoreClientShim shim = hc.metaStoreClientShim();
    // Thread.currentThread().setContextClassLoader(original);
    try {
      LOG.warn("ICEBERG!!!: Listing tables");
      String str = hc.getTablesByName("default", new Set.Set1<>("a").toSeq()).mkString();
      LOG.warn("ICEBERG!!!: Listed tables: " + str);
      // shim.getTables("default", ".*");
    } catch (Throwable t) {
      throw new RuntimeMetaException(t, "");
    }

    return hc;
    // return shim;

    //    try {
    //      try {
    //        return GET_CLIENT.invoke(
    //            hiveConf, (HiveMetaHookLoader) tbl -> null, HiveMetaStoreClient.class.getName());
    //      } catch (RuntimeException e) {
    //        // any MetaException would be wrapped into RuntimeException during reflection, so
    // let's
    //        // double-check type here
    //        if (e.getCause() instanceof MetaException) {
    //          throw (MetaException) e.getCause();
    //        }
    //        throw e;
    //      }
    //    } catch (MetaException e) {
    //      throw new RuntimeMetaException(e, "Failed to connect to Hive Metastore");
    //    } catch (Throwable t) {
    //      if (t.getMessage().contains("Another instance of Derby may have already booted")) {
    //        throw new RuntimeMetaException(
    //            t,
    //            "Failed to start an embedded metastore because embedded "
    //                + "Derby supports only one client at a time. To fix this, use a metastore that
    // supports "
    //                + "multiple clients.");
    //      }
    //
    //      throw new RuntimeMetaException(t, "Failed to connect to Hive Metastore");
    //    }
  }

  @Override
  protected HiveClient reconnect(HiveClient client) {
    try {
      //      client.close();
      //      client.reconnect();
    } catch (Throwable e) {
      throw new RuntimeMetaException(e, "Failed to reconnect to Hive Metastore");
    }
    return client;
  }

  @Override
  protected boolean isConnectionException(Exception e) {
    return super.isConnectionException(e)
        || (e != null
            && e instanceof MetaException
            && e.getMessage()
                .contains("Got exception: org.apache.thrift.transport.TTransportException"));
  }

  @Override
  protected void close(HiveClient client) {
    // client.close();
  }

  @VisibleForTesting
  HiveConf hiveConf() {
    return hiveConf;
  }
}
