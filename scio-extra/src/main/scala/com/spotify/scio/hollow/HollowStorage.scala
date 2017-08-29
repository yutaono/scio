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

package com.spotify.scio.hollow

import java.io.File

import com.netflix.hollow.api.consumer.HollowConsumer
import com.netflix.hollow.api.consumer.fs._
import com.netflix.hollow.api.producer.HollowProducer
import com.netflix.hollow.api.producer.HollowProducer.Populator
import com.netflix.hollow.api.producer.fs._
import com.spotify.scio.hollow.api.{KVAPI, KVPrimaryKeyIndex}
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.options.PipelineOptionsFactory

import scala.collection.JavaConverters._

trait HollowStorage {
  def publisher: HollowProducer.Publisher
  def announcer: HollowProducer.Announcer
  def retriever: HollowConsumer.BlobRetriever
  def watcher: HollowConsumer.AnnouncementWatcher
}

object HollowStorage {
  def forLocalFs(directory: File): HollowStorage = new HollowLocalFsStorage(directory)
  def forFs(directory: String, watcherIntervalSeconds: Int = 60): HollowStorage = {
    FileSystems.setDefaultPipelineOptions(PipelineOptionsFactory.create())
    new HollowFsStorage(FileSystems.matchNewResource(directory, true), watcherIntervalSeconds)
  }
}

private class HollowLocalFsStorage(val directory: File) extends HollowStorage {
  override def publisher: HollowProducer.Publisher = new HollowFilesystemPublisher(directory)
  override def announcer: HollowProducer.Announcer = new HollowFilesystemAnnouncer(directory)
  override def retriever: HollowConsumer.BlobRetriever =
    new HollowFilesystemBlobRetriever(directory)
  override def watcher: HollowConsumer.AnnouncementWatcher =
    new HollowFilesystemAnnouncementWatcher(directory)
}

class HollowKVWriter(storage: HollowStorage) {
  private val producer = {
    val p = HollowProducer
      .withPublisher(storage.publisher)
      .withAnnouncer(storage.announcer)
      .build()

    p.initializeDataModel(classOf[KV])
    p.restore(storage.watcher.getLatestVersion, storage.retriever)

    p
  }

  def write(iterable: Iterable[(Array[Byte], Array[Byte])]): Unit = write(iterable.iterator)

  def write(iterator: Iterator[(Array[Byte], Array[Byte])]): Unit =
    producer.runCycle(new Populator {
      override def populate(state: HollowProducer.WriteState): Unit = {
        while (iterator.hasNext) {
          val kv = iterator.next()
          state.add(KV.of(kv._1, kv._2))
        }
      }
    })
}

class HollowKVReader(storage: HollowStorage) {
  private val (api, idx) = {
    val consumer = HollowConsumer
      .withBlobRetriever(storage.retriever)
      .withAnnouncementWatcher(storage.watcher)
      .withGeneratedAPIClass(classOf[KVAPI])
      .build()
    consumer.triggerRefresh()
    (consumer.getAPI.asInstanceOf[KVAPI], new KVPrimaryKeyIndex(consumer))
  }

  def getString(key: String): String = {
    val v = idx.findMatch(key.getBytes)
    if (v == null) null else new String(v.getValue)
  }

  def getByteArray(key: Array[Byte]): Array[Byte] = idx.findMatch(key).getValue

  def iterable: Iterable[(Array[Byte], Array[Byte])] =
    api.getAllKV.asScala.map(kv => (kv.getKey, kv.getValue))

  def iterator: Iterator[(Array[Byte], Array[Byte])] =
    api.getAllKV.iterator().asScala.map(kv => (kv.getKey, kv.getValue))

}
