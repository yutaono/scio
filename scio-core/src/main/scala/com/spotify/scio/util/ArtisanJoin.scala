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

import java.lang.{Iterable => JIterable}

import com.google.common.collect.{AbstractIterator, Iterables}
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.transforms.DoFn.ProcessElement
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.join.{CoGbkResult, CoGroupByKey, KeyedPCollectionTuple}
import org.apache.beam.sdk.values.{KV, TupleTag}

import scala.reflect.ClassTag

private[scio] object ArtisanJoin {

  private def cogroup[KEY: ClassTag, A: ClassTag, B: ClassTag, A1, B1]
  (name: String, a: SCollection[(KEY, A)], b: SCollection[(KEY, B)])
  (leftFn: JIterable[A] => JIterable[A1], rightFn: JIterable[B] => JIterable[B1])
  : SCollection[(KEY, (A1, B1))] = {
    val (tagA, tagB) = (new TupleTag[A](), new TupleTag[B]())
    val keyed = KeyedPCollectionTuple
      .of(tagA, a.toKV.internal)
      .and(tagB, b.toKV.internal)
      .apply(name, CoGroupByKey.create())

    type DF = DoFn[KV[KEY, CoGbkResult], (KEY, (A1, B1))]
    a.context.wrap(keyed).withName(name)
      .applyTransform(ParDo.of(new DF {
        @ProcessElement
        private[util] def processElement(c: DF#ProcessContext): Unit = {
          val kv = c.element()
          val key = kv.getKey
          val result = kv.getValue
          val as = result.getAll(tagA)
          val bs = result.getAll(tagB)
          val i = new CartesianIterator(leftFn(as), rightFn(bs))
          while (i.hasNext) {
            c.output((key, i.next()))
          }
        }
      }))
  }

  def apply[KEY: ClassTag, A: ClassTag, B: ClassTag](name: String,
                                                     a: SCollection[(KEY, A)],
                                                     b: SCollection[(KEY, B)])
  : SCollection[(KEY, (A, B))] = cogroup(name, a, b)(identity, identity)

  def left[KEY: ClassTag, A: ClassTag, B: ClassTag](name: String,
                                                    a: SCollection[(KEY, A)],
                                                    b: SCollection[(KEY, B)])
  : SCollection[(KEY, (A, Option[B]))] = cogroup(name, a, b)(identity, toOptions)

  def right[KEY: ClassTag, A: ClassTag, B: ClassTag](name: String,
                                                     a: SCollection[(KEY, A)],
                                                     b: SCollection[(KEY, B)])
  : SCollection[(KEY, (Option[A], B))] = cogroup(name, a, b)(toOptions, identity)

  def outer[KEY: ClassTag, A: ClassTag, B: ClassTag](name: String,
                                                     a: SCollection[(KEY, A)],
                                                     b: SCollection[(KEY, B)])
  : SCollection[(KEY, (Option[A], Option[B]))] = cogroup(name, a, b)(toOptions, toOptions)

  private def toOptions[A](xs: JIterable[A]): JIterable[Option[A]] =
    if (xs.iterator().hasNext) {
      Iterables.transform(xs, new com.google.common.base.Function[A, Option[A]] {
        override def apply(input: A) = Option(input)
      })
    } else {
      java.util.Collections.singletonList(None)
    }

}

private class CartesianIterator[A, B](as: JIterable[A], bs: JIterable[B])
  extends AbstractIterator[(A, B)] {
  private val asi = as.iterator()
  private var bsi = bs.iterator()
  private var a: A = _

  if (asi.hasNext) {
    a = asi.next()
  } else {
    endOfData()
  }

  override def computeNext(): (A, B) = {
    if (!bsi.hasNext) {
      if (!asi.hasNext)  {
        endOfData()
      } else {
        a = asi.next()
        bsi = bs.iterator

        if (!bsi.hasNext) {
          endOfData()
        } else {
          (a, bsi.next())
        }
      }
    } else {
      (a, bsi.next())
    }
  }
}
