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

package org.alfer.spark.core

import java.util
import java.util.UUID

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase._
import org.apache.hadoop.hbase.client.{Admin, ConnectionFactory, Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.{SparkConf, SparkContext}


object HBaseTest {
  def main(args: Array[String]) {
    val sparkConf = new SparkConf()
      .setAppName("HBaseTest")
      .setMaster("local[4]")
    val sc = new SparkContext(sparkConf)
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "172.30.6.81")
    conf.set("hbase.zookeeper.property.clientPort", "2181")
    conf.set("zookeeper.znode.parent", "/hbase")
    conf.set(TableInputFormat.INPUT_TABLE, "demo01")

    val tableName = "demo01"
    // createSchemaTables(conf, "demo01")
     fakeData(conf, tableName, 10)
    readData(sc, conf)


    sc.stop()
  }

  //  def createNamespace(conf: Configuration, nameSpace: String): Unit = {
  //    val connection = ConnectionFactory.createConnection(conf)
  //    val admin = connection.getAdmin
  //    val namespaceDesc = new NamespaceDescriptor("test")
  //    admin.createNamespace(namespaceDesc)
  //    connection.close()
  //  }

  def readData(sc: SparkContext, conf: Configuration): Unit = {
    val hbaseRDD = sc.newAPIHadoopRDD(conf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result])
    hbaseRDD.foreach(println(_))
  }

  def fakeData(conf: Configuration, tableName: String, num: Int) {
    val connection = ConnectionFactory.createConnection(conf)
    val hTable = connection.getTable(TableName.valueOf(tableName))
    val putList = new util.ArrayList[Put]()
    for (i <- 1 until (num)) {
      val put = new Put(Bytes.toBytes(i + ""))
      val value = UUID.randomUUID().toString.replaceAll("-", "")
      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes(i + ""), Bytes.toBytes(value))
      putList.add(put)
    }
    hTable.put(putList)
    hTable.close()
    connection.close()

  }

  def createSchemaTables(conf: Configuration, tableName: String): Unit = {
    val connection = ConnectionFactory.createConnection(conf)
    val admin = connection.getAdmin
    val tableDesc = new HTableDescriptor(TableName.valueOf(tableName))
    tableDesc.addFamily(new HColumnDescriptor("f").setCompressionType(Algorithm.SNAPPY))
    createOrOverwrite(admin, tableDesc)
    connection.close()
  }

  def createOrOverwrite(admin: Admin, tableDesc: HTableDescriptor): Unit = {
    if (!admin.tableExists(tableDesc.getTableName)) {
      admin.createTable(tableDesc)
    }
  }
}
