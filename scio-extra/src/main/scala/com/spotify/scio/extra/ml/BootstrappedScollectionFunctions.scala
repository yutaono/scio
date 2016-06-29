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

import com.google.cloud.dataflow.sdk.values.PCollection
import com.spotify.scio.ScioContext
import com.spotify.scio.values.{PCollectionWrapper, SCollection}
import com.twitter.algebird.Batched

import scala.reflect.ClassTag

class BootstrappedScollectionFunctions[T: ClassTag] private[ml]
                                                    (val internal: PCollection[T],
                                                     private[scio] val context: ScioContext,
                                                     private[ml] val sample: SCollection[T],
                                                     private[ml] val sampled: Seq[SCollection[T]])
                                                    (implicit ev: Numeric[T])
  extends PCollectionWrapper[T] {

  protected val ct: ClassTag[T] = implicitly[ClassTag[T]]

  /**
   * Confidence interval - bootstrap percentile method:
   */
  def empiricalMean(left: Double, right: Double): SCollection[(Double, Double)] = {
    require(left >= 0.0, s"left side ($left) has to be bigger than 0.0")
    require(left < right, s"left side ($left) has to be bigger than right side ($right)")
    require(right <= 1.0, s"right side ($right) has to be smaller than 1.0")

    import com.twitter.algebird.Aggregator.toList

    val eMean = sample.mean.asSingletonSideInput

    val nbrOfSamples = sampled.size

    sampled
      .map(sample => sample.mean)
      .reduce((s1, s2) => s1.union(s2))
      .aggregate[Option[Batched[Double]], List[Double]](toList)
      .withSideInputs(eMean)
      .map { (r, c) =>
        val histo = r.sorted
        (2 * c(eMean) - histo((right * nbrOfSamples).toInt),
          2 * c(eMean) - histo((left * nbrOfSamples).toInt))
      }
      .toSCollection
  }

  /**
   * Confidence interval - bootstrap percentile method:
   */
  def percentileMean(left: Double, right: Double): SCollection[(Double, Double)] = {
    require(left >= 0.0, s"left side ($left) has to be bigger than 0.0")
    require(left < right, s"left side ($left) has to be bigger than right side ($right)")
    require(right <= 1.0, s"right side ($right) has to be smaller than 1.0")

    import com.twitter.algebird.Aggregator.toList

    val nbrOfSamples = sampled.size

    sampled
      .map(sample => sample.mean)
      .reduce((s1, s2) => s1.union(s2))
      .aggregate[Option[Batched[Double]], List[Double]](toList)
      .map { r =>
        val histo = r.sorted
        (histo((left * nbrOfSamples).toInt) , histo((right * nbrOfSamples).toInt))
      }
  }
}
