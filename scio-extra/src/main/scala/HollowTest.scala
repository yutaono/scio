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

import java.util.UUID

import com.spotify.scio._
import com.spotify.scio.hollow.{HollowKVReader, HollowKVWriter, HollowStorage}
import org.apache.beam.sdk.transforms.{DoFn, ParDo}
import org.apache.beam.sdk.transforms.DoFn.ProcessElement

// scalastyle:off
object HollowTest {
  val path = "gs://scio-playground-us/hollow-100m"
  val txt1 = "gs://scio-playground-us/hollow-txt1"
  val txt2 = "gs://scio-playground-us/hollow-txt2"
  val out = "gs://scio-playground-us/hollow-out"

  val baseArgs =
    """
      |--project=scio-playground --runner=DataflowRunner
      |--workerMachineType=n1-standard-8
      |--experiments=shuffle_mode=service
    """.stripMargin.trim.split("\\s+")

  def main(args: Array[String]): Unit = {
    args(0) match {
      case "write1" =>
        val args = baseArgs ++ s"--n=700 --out=$txt1".split("\\s+")
        HollowWriteTest1.main(args)
      case "write2" =>
        val args = baseArgs ++ s"--n=50 --in=$txt1 --out=$txt2".split("\\s+")
        HollowWriteTest1.main(args)
      case "read" =>
        val args = baseArgs ++ s"--in1=$txt1 --in2=$txt2 --out=$out".split("\\s+")
        HollowReadTest.main(args)
      case s => throw new RuntimeException(s"Unknown command $s")
    }
  }

  def write1(): Unit = {
    val args = baseArgs ++ s"--n=700 --out=$txt1".split("\\s+")
    HollowWriteTest1.main(args)
  }
}

object HollowWriteTest1 {
  def main(argz: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argz)
    val n = args.int("n")
    val out = args("out")
    val kvs = sc.parallelize(Seq.fill(n)(1000000L)).applyTransform(ParDo.of(new UUIDDoFn))
    kvs.map(kv => s"${kv._1}\t${kv._2}").saveAsTextFile(out)
    kvs
      .groupBy(_ => ())
      .map { case (_, xs) =>
        val storage = HollowStorage.forFs(HollowTest.path)
        val writer = new HollowKVWriter(storage)
        writer.write(xs.iterator.map(kv => (kv._1.getBytes, kv._2.getBytes)))
        ()
      }
    sc.close()
  }
}

object HollowWriteTest2 {
  def main(argz: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argz)
    val n = args.int("n")
    val in = args("in")
    val out = args("out")
    val kvs = sc.parallelize(Seq.fill(n)(1000000L)).applyTransform(ParDo.of(new UUIDDoFn))
    kvs.map(kv => s"${kv._1}\t${kv._2}").saveAsTextFile(out)
    val prev = sc.textFile(in).map { s =>
      val t = s.split("\t")
      (t(0), t(1))
    }
    (kvs ++ prev)
      .groupBy(_ => ())
      .map { case (_, xs) =>
        val storage = HollowStorage.forFs(HollowTest.path)
        val writer = new HollowKVWriter(storage)
        writer.write(xs.iterator.map(kv => (kv._1.getBytes, kv._2.getBytes)))
        ()
      }
    sc.close()
  }
}

object HollowReadTest {
  val reader = new HollowKVReader(HollowStorage.forFs(HollowTest.path))
  def main(argz: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argz)
    val in1 = args("in1")
    val in2 = args("in2")
    val out = args("out")
    val keys = (sc.textFile(in1) ++ sc.textFile(in2)).map(_.split("\t")(0))
    keys
      .map { k =>
        s"$k\t${Option(reader.getString(k)).getOrElse("null")}"
      }
      .saveAsTextFile(out)
    sc.close()
  }
}

class UUIDDoFn extends DoFn[Long, (String, String)] {
  @ProcessElement
  def processElement(c: DoFn[Long, (String, String)]#ProcessContext): Unit = {
    val n = c.element()
    var i = 0L
    while (i < n) {
      c.output((UUID.randomUUID().toString, UUID.randomUUID().toString))
      i += 1
    }
  }
}
