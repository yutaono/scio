/*
 * Copyright 2017 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
// scalastyle:off
package com.spotify.scio.hollow

import java.io.File
import java.util.logging.{Level, Logger}

import com.netflix.hollow.api.codegen.HollowAPIGenerator
import com.netflix.hollow.api.consumer.HollowConsumer
import com.netflix.hollow.api.consumer.fs.{HollowFilesystemAnnouncementWatcher, HollowFilesystemBlobRetriever}
import com.netflix.hollow.api.producer.HollowProducer
import com.netflix.hollow.api.producer.HollowProducer.Populator
import com.netflix.hollow.api.producer.fs.{HollowFilesystemAnnouncer, HollowFilesystemPublisher}
import com.netflix.hollow.core.write.HollowWriteStateEngine
import com.netflix.hollow.core.write.objectmapper.HollowObjectMapper
import com.spotify.scio.hollow.api.{KVAPI, KVPrimaryKeyIndex}
import org.apache.commons.io.FileUtils

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.reflect.ClassTag

object Test {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("com.netflix.hollow.core.read.engine.HollowBlobReader").setLevel(Level.OFF)
//    generateAPI[KV]()
    test()
  }

  def test(): Unit = {
    //    val temp = Files.createTempDirectory("hollow-").toFile
    val temp = new File("/tmp/hollow-temp")
    FileUtils.deleteDirectory(temp)
    println(temp.toString)

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      println("!!!!! FIRST WRITE")
      write(temp, (1 to 100).map(x => (s"key$x", s"val$x")))

      Thread.sleep(3000)

      println("!!!!! SECOND WRITE")
      write(temp, (1 to 105).map(x => (s"key$x", s"val$x")))
    }

    Thread.sleep(1000)
    read(temp)
  }

  private def write(file: File, data: Seq[(String, String)]): Unit = {
    val publisher = new HollowFilesystemPublisher(file)
    val announcer = new HollowFilesystemAnnouncer(file)
    val retriever = new HollowFilesystemBlobRetriever(file)
    val watcher = new HollowFilesystemAnnouncementWatcher(file)

    val producer = HollowProducer
      .withPublisher(publisher)
      .withAnnouncer(announcer)
      .build()

    producer.initializeDataModel(classOf[KV])
    producer.restore(watcher.getLatestVersion, retriever)

    producer.runCycle(new Populator {
      override def populate(state: HollowProducer.WriteState): Unit = {
        println(s"WRITE: prior state: ${state.getPriorState}, version: ${state.getVersion}")
        data.foreach(kv => state.add(KV.of(kv._1, kv._2)))
      }
    })
  }


  private def read(file: File): Unit = {
    val retriever = new HollowFilesystemBlobRetriever(file)
    val watcher = new HollowFilesystemAnnouncementWatcher(file)

    val consumer = HollowConsumer
      .withBlobRetriever(retriever)
      .withAnnouncementWatcher(watcher)
      .withGeneratedAPIClass(classOf[KVAPI])
      .build()

    consumer.triggerRefresh()
    val api = consumer.getAPI.asInstanceOf[KVAPI]

    val idx = new KVPrimaryKeyIndex(consumer)

    (1 to 5).foreach { x =>
      println(s"READ $x")
      api.getAllKV.asScala
        .takeRight(3)
        .foreach(kv => println(kv.getKey, kv.getValue))
      println(Option(idx.findMatch("key105")).map(kv => (kv.getKey, kv.getValue)))
      Thread.sleep(1000)
    }
  }

}
