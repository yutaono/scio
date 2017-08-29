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

package com.spotify.scio.hollow

import com.netflix.hollow.api.codegen.HollowAPIGenerator
import com.netflix.hollow.core.write.HollowWriteStateEngine
import com.netflix.hollow.core.write.objectmapper.HollowObjectMapper

object GenerateAPI {

  def main(args: Array[String]): Unit = {
    val pkg = "com.spotify.scio.hollow.api"
    val cls = Thread.currentThread().getContextClassLoader.loadClass(args(0))
    val api = cls.getSimpleName + "API"

    // scalastyle:off regex
    println(s"Generating API for $pkg.$api")
    // scalastyle:on regex

    val writeEngine = new HollowWriteStateEngine
    val mapper = new HollowObjectMapper(writeEngine)
    mapper.initializeTypeState(cls)
    val generator = new HollowAPIGenerator.Builder()
      .withAPIClassname(api)
      .withPackageName(pkg)
      .withDataModel(writeEngine)
      .build()
    generator.generateFiles("scio-extra/src/main/java/" + pkg.replaceAll("\\.", "/"))
  }

}
