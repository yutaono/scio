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

package com.spotify.scio.util


import org.apache.beam.sdk.options.PipelineOptions
import org.apache.beam.sdk.{PipelineResult, PipelineRunner}

private[scio] object RunnerSpec {

  def isDirectRunner(runner: Class[_ <: PipelineRunner[_ <: PipelineResult]]): Boolean =
    runner.getName == "org.apache.beam.runners.direct.DirectRunner"

  // FIXME: cover other runners
  def isLocalRunner(runner: Class[_ <: PipelineRunner[_ <: PipelineResult]]): Boolean =
    isDirectRunner(runner)

  // FIXME: cover other runners
  def isRemoteRunner(runner: Class[_ <: PipelineRunner[_ <: PipelineResult]]): Boolean =
    !isDirectRunner(runner)

  def parallelism(options: PipelineOptions): Unit = {

  }

}

trait RunnerSpec {
  val runnerClass: Class[_ <: PipelineRunner[_ <: PipelineResult]]
  def parallelism(options: PipelineOptions): Int
}

private class DirectRunnerSpec extends RunnerSpec {
  import org.apache.beam.runners.direct.DirectRunner
  import org.apache.beam.runners.direct.DirectOptions

  override val runnerClass: Class[_ <: PipelineRunner[_ <: PipelineResult]] = classOf[DirectRunner]
  override def parallelism(options: PipelineOptions): Int =
    options.as(classOf[DirectOptions]).getTargetParallelism
}

private class DataflowRunnerSpec extends RunnerSpec {
  import org.apache.beam.runners.dataflow.DataflowRunner
  import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions

  override val runnerClass: Class[_ <: PipelineRunner[_ <: PipelineResult]] =
    classOf[DataflowRunner]

  override def parallelism(options: PipelineOptions): Int = {
    val opt = options.as(classOf[DataflowPipelineOptions])
    val numWorkers = math.max(opt.getNumWorkers, opt.getMaxNumWorkers)
    numWorkers * opt.getNumberOfWorkerHarnessThreads
  }
}