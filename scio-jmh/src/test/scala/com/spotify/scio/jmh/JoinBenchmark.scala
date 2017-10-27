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
import java.util.{ Arrays => JArrays, ArrayList => JArrayList }
import java.util.concurrent.TimeUnit

import com.google.common.collect.AbstractIterator
import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@State(Scope.Thread)
class JoinBenchmark {

  private def genIterable(n: Int): JIterable[Int] = {
    val l = new Array[Int](n)
    (0 until n).foreach(i => l(i) = i)
    // ArrayList has specialized implementation of Iterator as opposed to Arrays.ArrayList
    new JArrayList(JArrays.asList(l: _*))
  }

  private val i1 = genIterable(1)
  private val i10 = genIterable(10)
  private val i100 = genIterable(100)

  @Benchmark def forYieldA(blackhole: Blackhole): Unit = forYield(i1, i10, blackhole)
  @Benchmark def forYieldB(blackhole: Blackhole): Unit = forYield(i10, i1, blackhole)
  @Benchmark def forYieldC(blackhole: Blackhole): Unit = forYield(i10, i100, blackhole)
  @Benchmark def forYieldD(blackhole: Blackhole): Unit = forYield(i100, i10, blackhole)

  @Benchmark def artisanA(blackhole: Blackhole): Unit = artisan(i1, i10, blackhole)
  @Benchmark def artisanB(blackhole: Blackhole): Unit = artisan(i10, i1, blackhole)
  @Benchmark def artisanC(blackhole: Blackhole): Unit = artisan(i10, i100, blackhole)
  @Benchmark def artisanD(blackhole: Blackhole): Unit = artisan(i100, i10, blackhole)

  @Benchmark def artisan2A(blackhole: Blackhole): Unit = artisan2(i1, i10, blackhole)
  @Benchmark def artisan2B(blackhole: Blackhole): Unit = artisan2(i10, i1, blackhole)
  @Benchmark def artisan2C(blackhole: Blackhole): Unit = artisan2(i10, i100, blackhole)
  @Benchmark def artisan2D(blackhole: Blackhole): Unit = artisan2(i100, i10, blackhole)

  @Benchmark def iteratorA(blackhole: Blackhole): Unit = iterator(i1, i10, blackhole)
  @Benchmark def iteratorB(blackhole: Blackhole): Unit = iterator(i10, i1, blackhole)
  @Benchmark def iteratorC(blackhole: Blackhole): Unit = iterator(i10, i100, blackhole)
  @Benchmark def iteratorD(blackhole: Blackhole): Unit = iterator(i100, i10, blackhole)

  def forYield(as: JIterable[Int], bs: JIterable[Int], blackhole: Blackhole): Unit = {
    import scala.collection.JavaConverters._

    val xs: TraversableOnce[(Int, Int)] =
      for (a <- as.asScala.iterator; b <- bs.asScala.iterator) yield (a, b)
    val i = xs.toIterator
    while (i.hasNext) blackhole.consume(i.next())
  }

  def artisan(as: JIterable[Int], bs: JIterable[Int], blackhole: Blackhole): Unit = {
    (peak(as), peak(bs)) match {
      case ((1, a), (1, b)) => blackhole.consume((a, b))
      case ((1, a), (2, _)) =>
        val i = bs.iterator()
        while (i.hasNext) blackhole.consume(i, i.next())
      case ((2, _), (1, b)) =>
        val i = as.iterator()
        while (i.hasNext) blackhole.consume((i.next(), b))
      case ((2, _), (2, _)) =>
        val i = as.iterator()
        while (i.hasNext) {
          val a = i.next()
          val j = bs.iterator()
          while (j.hasNext) {
            blackhole.consume((a, j.next()))
          }
        }
      case _ => ()
    }
  }

  def artisan2(as: JIterable[Int], bs: JIterable[Int], blackhole: Blackhole): Unit = {
    val ai = as.iterator()

    while (ai.hasNext) {
      val a = ai.next()
      val bi = bs.iterator()

      while (bi.hasNext) {
        blackhole.consume((a, bi.next()))
      }
    }
  }

  def iterator(as: JIterable[Int], bs: JIterable[Int], blackhole: Blackhole): Unit = {
    val i = new CartesianIterator(as, bs)

    while (i.hasNext) {
      blackhole.consume(i.next())
    }
  }

  @inline private def peak[A](xs: JIterable[A]): (Int, A) = {
    val i = xs.iterator()
    if (i.hasNext) {
      val a = i.next()
      if (i.hasNext) (2, null.asInstanceOf[A]) else (1, a)
    } else {
      (0, null.asInstanceOf[A])
    }
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
