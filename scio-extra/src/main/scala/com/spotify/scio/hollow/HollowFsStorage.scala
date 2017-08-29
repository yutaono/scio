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

import java.io.{FileNotFoundException, InputStream, OutputStream}
import java.nio.channels.Channels
import java.util.concurrent._
import java.util.{List => JList}

import com.netflix.hollow.api.consumer.HollowConsumer
import com.netflix.hollow.api.producer.HollowProducer
import com.netflix.hollow.api.producer.HollowProducer.Blob.Type
import com.netflix.hollow.api.producer.fs.HollowFilesystemAnnouncer
import org.apache.beam.sdk.io.FileSystems
import org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions
import org.apache.beam.sdk.io.fs.ResourceId
import org.apache.beam.sdk.util.MimeTypes
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.io.Source

private class HollowFsStorage(val directory: ResourceId, watcherIntervalSeconds: Int)
  extends HollowStorage {
  override def publisher: HollowProducer.Publisher = new HollowFsPublisher(directory)
  override def announcer: HollowProducer.Announcer = new HollowFsAnnouncer(directory)
  override def retriever: HollowConsumer.BlobRetriever = new HollowFsRetriever(directory)
  override def watcher: HollowConsumer.AnnouncementWatcher =
    new HollowFsWatcher(directory, watcherIntervalSeconds)
}

private class HollowFsPublisher(val blobStoreDir: ResourceId) extends HollowProducer.Publisher {
  private val logger = LoggerFactory.getLogger(this.getClass)
  override def publish(blob: HollowProducer.Blob): Unit = {
    val destination = blob.getType match {
      case Type.SNAPSHOT =>
        blobStoreDir.resolve(
          s"${blob.getType.prefix}-${blob.getToVersion}",
          StandardResolveOptions.RESOLVE_FILE)
      case Type.DELTA | Type.REVERSE_DELTA =>
        blobStoreDir.resolve(
          s"${blob.getType.prefix}-${blob.getFromVersion}-${blob.getToVersion}",
          StandardResolveOptions.RESOLVE_FILE)
    }
    logger.info(s"Publishing to $destination")
    val is = blob.newInputStream()
    val os = Channels.newOutputStream(FileSystems.create(destination, MimeTypes.BINARY))
    StreamUtil.copy(is, os)
    os.close()
  }
}

private class HollowFsAnnouncer(val publishDir: ResourceId) extends HollowProducer.Announcer {
  private val logger = LoggerFactory.getLogger(this.getClass)
  override def announce(stateVersion: Long): Unit = {
    val announceFile = publishDir.resolve(
      HollowFilesystemAnnouncer.ANNOUNCEMENT_FILENAME, StandardResolveOptions.RESOLVE_FILE)
    logger.info(s"Announcing to $announceFile")
    val os = Channels.newOutputStream(FileSystems.create(announceFile, MimeTypes.TEXT))
    os.write(stateVersion.toString.getBytes)
    os.close()
  }
}

private class HollowFsRetriever(val blobStoreDir: ResourceId) extends HollowConsumer.BlobRetriever {

  private val logger = LoggerFactory.getLogger(this.getClass)

  override def retrieveSnapshotBlob(desiredVersion: Long): HollowConsumer.Blob = {
    val exactFile = blobStoreDir.resolve(
      s"snapshot-$desiredVersion", StandardResolveOptions.RESOLVE_FILE)
    val exists = try {
      FileSystems.matchSingleFileSpec(exactFile.toString)
      true
    } catch {
      case _: FileNotFoundException => false
    }
    if (exists) {
      logger.info(s"Retrieving snapshot $exactFile")
      new FsBlob(exactFile, desiredVersion)
    } else {
      var maxVersionBeforeDesired = Long.MinValue
      var maxVersionBeforeDesiredResourceId: ResourceId = null
      val spec = blobStoreDir.resolve("snapshot-*", StandardResolveOptions.RESOLVE_FILE)
      FileSystems.`match`(spec.toString).metadata().asScala.foreach { m =>
        val resource = m.resourceId().toString
        val version = resource.substring(resource.lastIndexOf("-") + 1).toLong
        if (version < desiredVersion && version > maxVersionBeforeDesired) {
          maxVersionBeforeDesired = version
          maxVersionBeforeDesiredResourceId = m.resourceId()
        }
      }
      if (maxVersionBeforeDesired > Long.MinValue) {
        logger.info(s"Retrieving snapshot $maxVersionBeforeDesiredResourceId")
        new FsBlob(maxVersionBeforeDesiredResourceId, maxVersionBeforeDesired)
      } else {
        null
      }
    }
  }

  override def retrieveDeltaBlob(currentVersion: Long): HollowConsumer.Blob = try {
    val spec = blobStoreDir.resolve(
      s"delta-$currentVersion-*", StandardResolveOptions.RESOLVE_FILE)
    val m = FileSystems.`match`(spec.toString).metadata().asScala.head
    val resource = m.resourceId().toString
    val destinationVersion = resource.substring(resource.lastIndexOf("-") + 1).toLong
    logger.info(s"Retrieving delta $m")
    new FsBlob(m.resourceId(), currentVersion, destinationVersion)
  } catch {
    case _: Throwable => null
  }

  override def retrieveReverseDeltaBlob(currentVersion: Long): HollowConsumer.Blob = try {
    val spec = blobStoreDir.resolve(
      s"reversedelta-$currentVersion-*", StandardResolveOptions.RESOLVE_FILE)
    val m = FileSystems.`match`(spec.toString).metadata().asScala.head
    val resource = m.resourceId().toString
    val destinationVersion = resource.substring(resource.lastIndexOf("-") + 1).toLong
    logger.info(s"Retrieving reverse delta $m")
    new FsBlob(m.resourceId(), currentVersion, destinationVersion)
  } catch {
    case _: Throwable => null
  }

  private class FsBlob(val file: ResourceId, fromVersion: Long, toVersion: Long)
    extends HollowConsumer.Blob(fromVersion, toVersion) {
    def this(resourceId: ResourceId, toVersion: Long) = this(resourceId, Long.MinValue, toVersion)
    override def getInputStream: InputStream = Channels.newInputStream(FileSystems.open(file))
  }

}

// scalastyle:off no.finalize
private class HollowFsWatcher(val publishDir: ResourceId, intervalSeconds: Int)
  extends HollowConsumer.AnnouncementWatcher {

  private val logger = LoggerFactory.getLogger(this.getClass)

  private val announceFile = publishDir.resolve(
    HollowFilesystemAnnouncer.ANNOUNCEMENT_FILENAME, StandardResolveOptions.RESOLVE_FILE)
  private val subscribedConsumers: JList[HollowConsumer] =
    new CopyOnWriteArrayList[HollowConsumer]()
  private var latestVersion: Long = getCurrentVersion

  private val executor = Executors.newScheduledThreadPool(1, new ThreadFactory {
    override def newThread(r: Runnable): Thread = {
      val t = new Thread(r)
      t.setDaemon(true)
      t
    }
  })

  executor.scheduleWithFixedDelay(new Runnable {
    override def run(): Unit = {
      try {
        val currentVersion = getCurrentVersion
        if (latestVersion != currentVersion && currentVersion > Long.MinValue) {
          logger.info(s"New version available $currentVersion")
          latestVersion = currentVersion
          subscribedConsumers.asScala.foreach(_.triggerAsyncRefresh())
        }
      } catch {
        case _: Throwable => Unit
      }
    }
  }, 0, intervalSeconds, TimeUnit.SECONDS)

  override def finalize(): Unit = {
    super.finalize()
    executor.shutdown()
  }

  override def subscribeToUpdates(consumer: HollowConsumer): Unit =
    subscribedConsumers.add(consumer)

  override def getLatestVersion: Long = latestVersion

  private def getCurrentVersion: Long = try {
    val is = Channels.newInputStream(FileSystems.open(announceFile))
    Source.fromInputStream(is).getLines().next().toLong
  } catch {
    case _: Throwable => Long.MinValue
  }

}
// scalastyle:on no.finalize

private object StreamUtil {
  def copy(is: InputStream, os: OutputStream): Unit = {
    val buf = new Array[Byte](4096)
    var n = is.read(buf)
    while (-1 != n) {
      os.write(buf, 0, n)
      n = is.read(buf)
    }
  }
}
