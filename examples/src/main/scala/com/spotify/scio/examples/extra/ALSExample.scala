package com.spotify.scio.examples.extra

import breeze.linalg._
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.google.cloud.dataflow.sdk.options.PipelineOptions.CheckEnabled
import com.spotify.scio._
import com.spotify.scio.ml.recommendation._
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.Duration

object ALSExample {

  def main(cmdlineArgs: Array[String]): Unit = {

    val (opts, args) = ScioContext.parseArguments[DataflowPipelineOptions](cmdlineArgs)
    opts.setStableUniqueNames(CheckEnabled.OFF)

    val iterations = args.int("iterations")
    val rank = args.int("rank")
    val ratingsFile = args("ratings")
    val itemsFile = args("items")
    val movieId = args.int("movieId", 50)  // Star Wars

    val ratings = getRatings(opts, ratingsFile)

    val lambda = 0.01
    val alpha = 1.0

    val model = ALS.train(ratings, opts, iterations, rank, alpha, lambda, implicitPrefs = true)

    val result = getResult(opts, model, itemsFile, movieId)

    val logger = LoggerFactory.getLogger(this.getClass)
    Await.result(result, Duration.Inf).value.toSeq
      .sortBy(_._2._1)
      .map(t => "%8.6f: %s".format(t._2._1, t._2._2))
      .foreach(logger.info)
  }

  private def getRatings(opts: DataflowPipelineOptions, ratingsFile: String) = {
    val sc = ScioContext(opts)
    val r = sc
      .textFile(ratingsFile)
      .map { s =>
        val t = s.split("\t")
        Rating(t(0).toInt, t(1).toInt, t(2).toDouble)
      }.materialize
    sc.close()
    r
  }

  private def getResult(opts: DataflowPipelineOptions,
                        model: MatrixFactorizationModel,
                        itemsFile: String,
                        movieId: Int) = {
    val sc = ScioContext(opts)

    val itemVectors = Await.result(model.itemVectors, Duration.Inf).open(sc)
    val items = sc
      .textFile(itemsFile)
      .map { s =>
        val t = s.split("\\|")
        (t(0).toInt, t(1))
      }

    val r = itemVectors
      .cross(itemVectors.filter(_._1 == movieId).map(_._2))
      .map { t =>
        val ((id, v1), v2) = t
        val cosine: Double = (v1 dot v2) / (norm(v1) * norm(v2))
        (id, cosine)
      }
      .top(10)(Ordering.by(-_._2))
      .flatMap(identity)
      .join(items)
      .materialize

    sc.close()
    r
  }

}
