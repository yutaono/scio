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

import org.apache.beam.runners.dataflow.DataflowRunner
import org.apache.beam.runners.direct.DirectRunner
import org.scalatest._

class RunnerSpecTest extends FlatSpec with Matchers {

  "RunnerSpec" should "support DirectRunner" in {
    val runner = classOf[DirectRunner]
    RunnerSpec.isDirectRunner(runner) shouldBe true
    RunnerSpec.isLocalRunner(runner) shouldBe true
    RunnerSpec.isRemoteRunner(runner) shouldBe false
  }

  it should "support DataflowRunner" in {
    val runner = classOf[DataflowRunner]
    RunnerSpec.isDirectRunner(runner) shouldBe false
    RunnerSpec.isLocalRunner(runner) shouldBe false
    RunnerSpec.isRemoteRunner(runner) shouldBe true
  }

}
