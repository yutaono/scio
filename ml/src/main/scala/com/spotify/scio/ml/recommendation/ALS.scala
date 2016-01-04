package com.spotify.scio.ml.recommendation

import breeze.linalg._
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.spotify.scio._
import com.spotify.scio.io.Tap
import com.spotify.scio.values.{SideOutput, SCollection}
import com.twitter.algebird.Semigroup
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class Rating(user: Int, item: Int, rating: Double)

case class MatrixFactorizationModel(rank: Int,
                                    userVectors: Future[Tap[(Int, DenseVector[Double])]],
                                    itemVectors: Future[Tap[(Int, DenseVector[Double])]])

object ALS {

  def train(ratings: Future[Tap[Rating]],
            options: DataflowPipelineOptions,
            iterations: Int,
            rank: Int,
            alpha: Double,
            lambda: Double,
            implicitPrefs: Boolean): MatrixFactorizationModel = {
    new ALS(ratings, options, iterations, rank, alpha, lambda, implicitPrefs).run()
  }

}

private class ALS(val ratings: Future[Tap[Rating]],
                  val options: DataflowPipelineOptions,
                  val iterations: Int,
                  val rank: Int,
                  val alpha: Double,
                  val lambda: Double,
                  val implicitPrefs: Boolean) {

  private type R = Rating
  private type V = DenseVector[Double]
  private type M = DenseMatrix[Double]
  private type FT[U] = Future[Tap[U]]
  private type FTV = FT[(Int, V)]

  private val logger = LoggerFactory.getLogger(classOf[ALS])

  implicit private val vectorSemigroup: Semigroup[V] =
    new Semigroup[V] {
      override def plus(l: V, r: V): V = l + r
      override def sumOption(iter: TraversableOnce[V]): Option[V] = {
        var x: V = null
        iter.foreach { y =>
          if (x == null) {
            x = y.copy
          } else {
            x :+= y
          }
        }
        Option(x)
      }
    }

  implicit private val matrixSemigroup: Semigroup[M] =
    new Semigroup[M] {
      override def plus(l: M, r: M): M = l + r
      override def sumOption(iter: TraversableOnce[M]): Option[M] = {
        var x: M = null
        iter.foreach { y =>
          if (x == null) {
            x = y.copy
          } else {
            x :+= y
          }
        }
        Option(x)
      }
    }

  // Functions for extracting keys
  private def userKey: R => Int = _.user
  private def itemKey: R => Int = _.item

  // scalastyle:off
  private def updateFeatures(ratings: SCollection[R],
                             fixedVectors: SCollection[(Int, V)],
                             user: Boolean): SCollection[(Int, V)] = {
    val solveKey = if (user) userKey else itemKey
    val fixedKey = if (user) itemKey else userKey
    val lambdaEye = diag(DenseVector.ones[Double](rank)) * lambda
    val unitMatrix = DenseMatrix.zeros[Double](1, 1)

    // FIXME: workaround for nulls in closure
    val _implicitPrefs = this.implicitPrefs
    val _alpha = this.alpha

    val p = ratings.keyBy(fixedKey).join(fixedVectors).values

    if (implicitPrefs) {
      val sums = p.map { case (r, vec) =>
        val op = vec * vec.t
        val cui = 1.0 + _alpha * r.rating
        val pui = if (cui > 0.0) 1.0 else 0.0
        val ytCuIY = op * (_alpha * r.rating)
        val ytCupu = vec * (cui * pui)
        (solveKey(r), (ytCuIY, ytCupu, op))
      }
      val yty = sums.map(_._2._3).sum  // sum outer product globally for YtY
      sums.cross(yty)
        .map { t =>
          val ((id, (ytCuIY, ytCupu, _)), yty) = t
          val xu = (yty + ytCuIY + lambdaEye) \ ytCupu
          (id, xu)
        }
    } else {
      val sums = p.map { case (r, vec) =>
        val (ytCupu, op) = (vec * r.rating, vec * vec.t)
        (solveKey(r), (ytCupu, op))
      }
      val yty = sums.map(_._2._2).sum  // sum outer product globally for YtY
      sums.cross(yty)
        .map { t =>
          val ((id, (ytCupu, _)), yty) = t
          val xu = (yty + lambdaEye) \ ytCupu
          (id, xu)
        }
    }

    /*
    // sum things up by fixed key
    val sums = ratings.keyBy(fixedKey).join(fixedVectors)
      .map { kv =>
        val (r, vec) = kv._2
        val op = vec * vec.t
        val id = solveKey(r)
        val (ytCuIY, ytCupu) = if (_implicitPrefs) {
          val cui = 1.0 + _alpha * r.rating
          val pui = if (cui > 0.0) 1.0 else 0.0
          val ytCuIY = op * (_alpha * r.rating)
          val ytCupu = vec * (cui * pui)
          (ytCuIY, ytCupu)
        } else {
          // c_ui = 1
          // p_ui = r_ui
          (unitMatrix, vec * r.rating)
        }
        (id, (ytCuIY, ytCupu, op))
      }
      .sumByKey

    val yty = sums.map(_._2._3).sum  // sum outer product globally for YtY
    sums.cross(yty)
      .map { t =>
        val ((id, (ytCuIY, ytCupu, _)), yty) = t
        val xu = if (_implicitPrefs) {
          (yty + ytCuIY + lambdaEye) \ ytCupu
        } else {
          (yty + lambdaEye) \ ytCupu
        }
        (id, xu)
      }
      */
  }

  private def runIteration(currentIteration: Int, userVectors: FTV, itemVectors: FTV): (FTV, FTV) = {
    if (currentIteration > iterations) {
      (userVectors, itemVectors)
    } else {
      val sc = ScioContext(options)
      sc.setName(options.getAppName + currentIteration + "of" + iterations)
      val r = Await.result(ratings, Duration.Inf).open(sc)
      val u = Await.result(userVectors, Duration.Inf).open(sc)
      val i = Await.result(itemVectors, Duration.Inf).open(sc)

      logger.info(s"Running iteration $currentIteration of $iterations")
      val userF = updateFeatures(r, i, user = true).materialize
      val itemF = updateFeatures(r, u, user = false).materialize
      sc.close()

      runIteration(currentIteration + 1, userF, itemF)
    }
  }

  private def prepareVectors(): (FTV, FTV) = {
    val rank = this.rank
    val sc = ScioContext(options)
    sc.setName(options.getAppName + "prepare")
    val r = Await.result(ratings, Duration.Inf).open(sc)
    logger.info("Preparing vectors")
    val userVectors = r.map(_.user).distinct().map((_, DenseVector.rand[Double](rank))).materialize
    val itemVectors = r.map(_.item).distinct().map((_, DenseVector.rand[Double](rank))).materialize
    sc.close()
    (userVectors, itemVectors)
  }

  def run(): MatrixFactorizationModel = {
    val (u, i) = prepareVectors()
    val (uOut, iOut) = runIteration(1, u, i)
    MatrixFactorizationModel(rank, uOut, iOut)
  }

}
