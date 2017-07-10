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

package com.spotify.scio

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.client.util.ExponentialBackOff
import com.google.api.services.{compute => gce}
import com.google.api.services.compute.model._
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.util.Random

// scalastyle:off
object ComputeTest {
  def opts = {
    val TIME_FORMATTER = DateTimeFormat.forPattern("yyyyMMddHHmmss")
    val now = Instant.now().toString(TIME_FORMATTER)
    val instanceName = "scio-tf-" + now + "-" + Random.nextInt(Int.MaxValue)
    CreateOptions("scio-playground", "us-east1-d", instanceName, "scio-playground", "tfimage")
  }
  val compute = new Compute
}

case class CreateOptions(projectId: String, zone: String, instanceName: String,
                         imageProject: String, imageFamily: String,
                         machineType: String = "n1-standard-4",
                         diskSize: Long = 500L, diskType: String = "pd-ssd",
                         acceleratorType: String = "nvidia-tesla-k80", acceleratorCount: Int = 1)

class ComputeException(msg: String) extends Exception(msg)

class Compute {

  import scala.concurrent.ExecutionContext.Implicits.global

  private val rootUrl = "https://www.googleapis.com/compute/beta"
  private val compute = new gce.Compute.Builder(
    GoogleNetHttpTransport.newTrustedTransport(),
    JacksonFactory.getDefaultInstance,
    GoogleCredential.getApplicationDefault())
    .build()

  def create(opts: CreateOptions): Future[Operation] = {
    val baseUrl = s"$rootUrl/projects/${opts.projectId}/zones/${opts.zone}"
    val machineType = s"zones/${opts.zone}/machineTypes/${opts.machineType}"
    val sourceImage = s"projects/${opts.imageProject}/global/images/family/${opts.imageFamily}"
    val acceleratorType = s"$baseUrl/acceleratorTypes/${opts.acceleratorType}"

    val instance = new Instance()
      .setName(opts.instanceName)
      .setMachineType(machineType)
      .setNetworkInterfaces(List(new NetworkInterface()).asJava)
      .setDisks(List(new AttachedDisk()
        .setAutoDelete(true)
        .setBoot(true)
        .setInitializeParams(new AttachedDiskInitializeParams()
          .setDiskSizeGb(opts.diskSize)
          .setDiskType(s"$baseUrl/diskTypes/${opts.diskType}")
          .setSourceImage(sourceImage))).asJava)
      .setGuestAccelerators(List(new AcceleratorConfig()
        .setAcceleratorType(acceleratorType)
        .setAcceleratorCount(opts.acceleratorCount)).asJava)
      .setScheduling(new Scheduling()
        .setOnHostMaintenance("TERMINATE"))
    val op = compute.instances().insert(opts.projectId, opts.zone, instance).execute()
    backOff(() => compute.zoneOperations.get(opts.projectId, opts.zone, op.getName).execute())
  }

  def delete(projectId: String, zone: String, instanceName: String): Future[Operation] = {
    val op = compute.instances().delete(projectId, zone, instanceName).execute()
    backOff(() => compute.zoneOperations.get(projectId, zone, op.getName).execute())
  }

  private def backOff(f: () => Operation): Future[Operation] = Future {
    val backOff = new ExponentialBackOff()
    var op = f()
    while (op.getStatus != "DONE" && op.getError == null) {
      Thread.sleep(backOff.nextBackOffMillis())
      op = f()
    }
    if (op.getError != null) {
      throw new ComputeException(op.getError.toString)
    }
    op
  }
}
