/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.extra.ml

import com.spotify.scio.values.SCollection

import scala.reflect.ClassTag

private[ml] class BootstrapSCollection[T: ClassTag](self: SCollection[T])
                                                   (implicit ev: Numeric[T]) {
  def bootstrap(samples: Int, fraction: Double): BootstrappedScollectionFunctions[T] = {
    require(samples > 0, "Number of samples must be more than 0")
    require(fraction > 0.0 && fraction <= 1.0, "Fraction must belong to interval (0.0, 1.0]")

    // initial sample which might be whole dataset if the data was presampled
    val initialSample = self
      .sample(withReplacement = false, fraction)
      .setName("Initial sample for boostrapping")

    // resampling:
    val sampled = (1 to samples)
      .map(i => initialSample.sample(withReplacement = true, 1.0)
      .setName(s"Resample number $i"))

    new BootstrappedScollectionFunctions[T](self.internal, self.context, initialSample, sampled)
  }
}
