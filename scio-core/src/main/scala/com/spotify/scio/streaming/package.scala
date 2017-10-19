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

import com.spotify.scio.util.Functions
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.GenerateSequence
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{Duration, Instant}

/**
 * Main package for streaming APIs. Import all.
 *
 * {{{
 * import com.spotify.scio.streaming._
 * }}}
 */
package object streaming {

  /** Alias for WindowingStrategy `AccumulationMode.ACCUMULATING_FIRED_PANES`. */
  val ACCUMULATING_FIRED_PANES = AccumulationMode.ACCUMULATING_FIRED_PANES

  /** Alias for WindowingStrategy `AccumulationMode.DISCARDING_FIRED_PANES`. */
  val DISCARDING_FIRED_PANES = AccumulationMode.DISCARDING_FIRED_PANES

  /** Enchanced version of [[ScioContext]] with streaming methods. */
  implicit class StreamingScioContext(val self: ScioContext) extends AnyVal {
    /**
     * Generate a sequence of longs starting from the given value, and either up to the given limit
     * or until [[Long.MaxValue]] / until the given time elapses.
     * @param from the minimum number to generate (inclusive)
     * @param to the maximum number to generate (exclusive)
     * @param interval interval between elements
     * @param maxReadTime stop generating elements after the given time
     * @param timestampFn function to use to assign timestamps to the elements
     * @return
     */
    def sequence(from: Long, to: Long = -1,
                 interval: Duration = null,
                 maxReadTime: Duration = null,
                 timestampFn: Long => Instant = null): SCollection[Long] = {
      var t = GenerateSequence.from(from).to(to)
      if (interval != null) {
        t = t.withRate(1, interval)
      }
      if (maxReadTime != null) {
        t = t.withMaxReadTime(maxReadTime)
      }
      if (timestampFn != null) {
        val f = Functions.serializableFn(timestampFn.asInstanceOf[java.lang.Long => Instant])
        t = t.withTimestampFn(f)
      }
      self.customInput("sequence", t).asInstanceOf[SCollection[Long]]
    }
  }
}
