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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.dataflow.Dataflow
import com.google.api.services.dataflow.model.DisplayData

import scala.collection.JavaConverters._

object DataflowUtil {

  private lazy val df = {
    val transport = GoogleNetHttpTransport.newTrustedTransport()
    val jackson = JacksonFactory.getDefaultInstance
    val credential = GoogleCredential.getApplicationDefault
    new Dataflow.Builder(transport, jackson, credential).build()
  }

  def main(args: Array[String]): Unit = {
    val projectId = "scio-playground"
//    val jobId = "2017-07-31_10_32_37-7016036794321214616"
    val jobId = "2017-08-01_13_49_13-15666807526565315016"

    println(commandLine(projectId, jobId, false))
    println(commandLine(projectId, jobId, true))
  }

  def commandLine(projectId: String, jobId: String, isStreaming: Boolean = false): String = {
    val job = df.projects().jobs().get(projectId, jobId).setView("JOB_VIEW_ALL")
      .execute()

    val dd = job.getPipelineDescription.getDisplayData.asScala
    val opts = dd
      .filter { d =>
        d.getNamespace != "com.spotify.scio.options.ScioOptions" && d.getKey != "appName" &&
          (isStreaming || d.getKey != "jobName")
      }
      .map(d => s"--${d.getKey}=${getValue(d)}")
    val streamingOpts = if (isStreaming) Some("--update") else None
    val args = dd.find(_.getKey == "appArguments").map(_.getStrValue.replaceAll(", --", " --"))
    (opts ++ streamingOpts ++ args).mkString(" ")
  }

  private def getValue(d: DisplayData): String = {
    if (d.getBoolValue != null) {
      d.getBoolValue
    } else if (d.getInt64Value != null) {
      d.getInt64Value
    }  else if (d.getFloatValue != null) {
      d.getFloatValue
    } else if (d.getStrValue != null) {
      d.getStrValue
    } else if (d.getShortStrValue != null) {
      d.getShortStrValue
    } else if (d.getDurationValue != null) {
      d.getDurationValue
    } else if (d.getTimestampValue != null) {
      d.getTimestampValue
    } else if (d.getJavaClassValue != null) {
      d.getJavaClassValue
    } else {
      "null"
    }
  }.toString

}
