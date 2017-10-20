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

package com.spotify.scio.jmh

import java.lang.{Iterable => JIterable}
import java.util.concurrent.TimeUnit

import com.google.common.collect.AbstractIterator
import com.google.common.collect.Lists
import org.openjdk.jmh.annotations._

import scala.collection.JavaConverters._

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class JoinBenchmark {

  private def genIterable(n: Int): JIterable[Int] = {
    val l = Lists.newArrayList[Int]()
    (1 to n).foreach(l.add)
    l
  }

  private val i1 = genIterable(1)
  private val i10 = genIterable(10)
  private val i100 = genIterable(100)

  @Benchmark def forYieldA: Unit = forYield(i1, i10)
  @Benchmark def forYieldB: Unit = forYield(i10, i1)
  @Benchmark def forYieldC: Unit = forYield(i10, i100)
  @Benchmark def forYieldD: Unit = forYield(i100, i10)

  @Benchmark def artisanA: Unit = artisan(i1, i10)
  @Benchmark def artisanB: Unit = artisan(i10, i1)
  @Benchmark def artisanC: Unit = artisan(i10, i100)
  @Benchmark def artisanD: Unit = artisan(i100, i10)

  @Benchmark def iteratorA: Unit = artisan(i1, i10)
  @Benchmark def iteratorB: Unit = artisan(i10, i1)
  @Benchmark def iteratorC: Unit = artisan(i10, i100)
  @Benchmark def iteratorD: Unit = artisan(i100, i10)

  def forYield(as: JIterable[Int], bs: JIterable[Int]): Unit =
    forYield(as, bs, new NoopContext[(Int, Int)])

  def forYield(as: JIterable[Int], bs: JIterable[Int], c: Context[(Int, Int)]): Unit = {
    val xs: TraversableOnce[(Int, Int)] =
      for (a <- as.asScala.iterator; b <- bs.asScala.iterator) yield (a, b)
    val i = xs.toIterator
    while (i.hasNext) c.output(i.next())
  }

  def artisan(as: JIterable[Int], bs: JIterable[Int]): Unit =
    artisan(as, bs, new NoopContext[(Int, Int)])

  def artisan(as: JIterable[Int], bs: JIterable[Int], c: Context[(Int, Int)]): Unit = {
    (peak(as), peak(bs)) match {
      case ((1, a), (1, b)) => c.output(a, b)
      case ((1, a), (2, _)) =>
        val i = bs.iterator()
        while (i.hasNext) c.output(a, i.next())
      case ((2, _), (1, b)) =>
        val i = as.iterator()
        while (i.hasNext) c.output(i.next(), b)
      case ((2, _), (2, _)) =>
        val i = as.iterator()
        while (i.hasNext) {
          val a = i.next()
          val j = bs.iterator()
          while (j.hasNext) {
            c.output(a, j.next())
          }
        }
      case _ => ()
    }
  }

  def iterator(as: JIterable[Int], bs: JIterable[Int]): Unit =
    iterator(as, bs, new NoopContext[(Int, Int)])

  def iterator(as: JIterable[Int], bs: JIterable[Int], c: Context[(Int, Int)]): Unit = {
    val i = new CartesianIterator(as, bs)
    while (i.hasNext) {
      c.output(i.next())
    }
  }

  @inline private def peak[A](xs: java.lang.Iterable[A]): (Int, A) = {
    val i = xs.iterator()
    if (i.hasNext) {
      val a = i.next()
      if (i.hasNext) (2, null.asInstanceOf[A]) else (1, a)
    } else {
      (0, null.asInstanceOf[A])
    }
  }

  test(i1, i1)
  test(i1, i10)
  test(i1, i100)
  test(i10, i1)
  test(i10, i10)
  test(i10, i100)
  test(i100, i1)
  test(i100, i10)
  test(i100, i100)

  private def test(as: JIterable[Int], bs: JIterable[Int]): Unit = {
    val c1 = new SeqContext[(Int, Int)]
    forYield(as, bs, c1)
    val c2 = new SeqContext[(Int, Int)]
    artisan(as, bs, c2)
    val c3 = new SeqContext[(Int, Int)]
    iterator(as, bs, c3)
    require(c1.result == c2.result)
    require(c1.result == c3.result)
  }

  trait Context[T] {
    def output(x: T): Unit
    def result: Seq[T]
  }

  class NoopContext[T] extends Context[T] {
    override def output(x: T): Unit = Unit
    override def result: Seq[T] = Nil
  }

  class SeqContext[T] extends Context[T] {
    private val b = Seq.newBuilder[T]
    override def output(x: T): Unit = b += x
    override def result: Seq[T] = b.result()
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
