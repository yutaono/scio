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
// scalastyle:off
package com.spotify.scio.hollow

import java.io.File
import java.util.logging.{Level, Logger}

import org.apache.commons.io.FileUtils

import scala.concurrent.Future

object Test {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("com.netflix.hollow.core.read.engine.HollowBlobReader").setLevel(Level.OFF)
//    generateAPI[KV]()
    test()
  }

  def test(): Unit = {
//    val temp = Files.createTempDirectory("hollow-").toFile
//    val temp = new File("/tmp/hollow-temp")
//    FileUtils.deleteDirectory(temp)
//    println(temp.toString)
//    val storage = HollowStorage.forLocalFs(temp)

    val storage = HollowStorage.forFs("gs://scio-playground-us/hollow")
    val writer = new HollowKVWriter(storage)

    import scala.concurrent.ExecutionContext.Implicits.global
    Future {
      println("!!!!! FIRST WRITE")
      writer.write((1 to 100).map(x => (s"key$x".getBytes(), s"val$x".getBytes())))

      Thread.sleep(5000)

      println("!!!!! SECOND WRITE")
      writer.write((1 to 105).map(x => (s"key$x".getBytes(), s"val$x".getBytes())))
    }

    Thread.sleep(2000)

    val reader = new HollowKVReader(storage)
    (1 to 5).foreach { x =>
      println(s"READ $x")
      reader.iterable
        .takeRight(3)
        .foreach(kv => println(new String(kv._1), new String(kv._2)))
      println(reader.getString("key105"))
      Thread.sleep(3000)
    }
  }

}
