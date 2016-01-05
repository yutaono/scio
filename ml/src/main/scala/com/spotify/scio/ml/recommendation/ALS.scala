package com.spotify.scio.ml.recommendation

import breeze.linalg._
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions
import com.spotify.scio._
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.twitter.algebird.Semigroup
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

case class Rating(user: Int, item: Int, rating: Double)

case class MatrixFactorizationModel(rank: Int,
                                    userVectors: Future[Tap[(Int, DenseVector[Double])]],
                                    itemVectors: Future[Tap[(Int, DenseVector[Double])]])

private object KeyType extends Enumeration {
  type KeyType = Value
  val USER_KEY, ITEM_KEY = Value
}

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

  import KeyType._

  private type R = Rating
  private type V = DenseVector[Double]
  private type M = DenseMatrix[Double]
  private type FT[U] = Future[Tap[U]]
  private type FTV = FT[(Int, V)]
  private type FTKR = FT[((KeyType, Int), R)]
  private type FTKV = FT[((KeyType, Int), V)]

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

    // FIXME: workaround for nulls in closure
    val _alpha = this.alpha

    val p = ratings.keyBy(fixedKey).join(fixedVectors).values

    if (implicitPrefs) {
      val sums = p
        .map { case (r, vec) =>
          val op = vec * vec.t
          val cui = 1.0 + _alpha * r.rating
          val pui = if (cui > 0.0) 1.0 else 0.0
          val ytCuIY = op * (_alpha * r.rating)
          val ytCupu = vec * (cui * pui)
          (solveKey(r), (ytCuIY, ytCupu, op))
        }
        .sumByKey
      val yty = sums.map(_._2._3).sum  // sum outer product globally for YtY
      sums.cross(yty)
        .map { t =>
          val ((id, (ytCuIY, ytCupu, _)), yty) = t
          val xu = (yty + ytCuIY + lambdaEye) \ ytCupu
          (id, xu)
        }
    } else {
      val sums = p
        .map { case (r, vec) =>
          val (ytCupu, op) = (vec * r.rating, vec * vec.t)
          (solveKey(r), (ytCupu, op))
        }
        .sumByKey
      val yty = sums.map(_._2._2).sum  // sum outer product globally for YtY
      sums.cross(yty)
        .map { t =>
          val ((id, (ytCupu, _)), yty) = t
          val xu = (yty + lambdaEye) \ ytCupu
          (id, xu)
        }
    }
  }

  private def updateFeatures(keyedRatings: SCollection[((KeyType, Int), R)],
                            vectors: SCollection[((KeyType, Int), V)]): SCollection[((KeyType, Int), V)] = {
    val lambdaEye = diag(DenseVector.ones[Double](rank)) * lambda

    val solveKey = (fixedKeyType: KeyType, r: R) => fixedKeyType match {
      case USER_KEY => (ITEM_KEY, r.item)
      case ITEM_KEY => (USER_KEY, r.user)
    }

    // FIXME: workaround for nulls in closure
    val _alpha = this.alpha

    val sums = keyedRatings.join(vectors)
      .map { case ((fixedKeyType, fixedKey), (r, vec)) =>
        val op = vec * vec.t
        val cui = 1.0 + _alpha * r.rating
        val pui = if (cui > 0.0) 1.0 else 0.0
        val ytCuIY = op * (_alpha * r.rating)
        val ytCupu = vec * (cui * pui)
        (solveKey(fixedKeyType, r), (ytCuIY, ytCupu, op))
      }
      .sumByKey

    val ytySide = sums.map(kv => (kv._1._1, kv._2._3)).sumByKey.asMapSideInput

    sums.withSideInputs(ytySide)
      .map { case (((solvedKeyType, solvedKey), (ytCuIY, ytCupu, op)), side) =>
        val yty = side(ytySide)(solvedKeyType)
        val xu = (yty + ytCuIY + lambdaEye) \ ytCupu
        ((solvedKeyType, solvedKey), xu)
      }
      .toSCollection
  }

  private def runIteration(currentIteration: Int, input: (FTKR, FTKV)): (FTKR, FTKV) = {
    if (currentIteration > iterations) {
      input
    } else {
      val sc = ScioContext(options)
      sc.setName(options.getAppName + currentIteration + "of" + iterations)
      val r = Await.result(input._1, Duration.Inf).open(sc)
      val v = Await.result(input._2, Duration.Inf).open(sc)

      logger.info(s"Running iteration $currentIteration of $iterations")
      val v2 = updateFeatures(r, v).materialize
      val t1 = System.currentTimeMillis()
      sc.close()
      val t2 = System.currentTimeMillis()
      logger.info(s"TIME: ${(t2 - t1) / 1000.0}s")

      runIteration(currentIteration + 1, (input._1, v2))
    }
  }

  private def prepareInput(): (FTKR, FTKV) = {
    val rank = this.rank
    val sc = ScioContext(options)
    sc.setName(options.getAppName + "prepare")
    val r = Await.result(ratings, Duration.Inf).open(sc)
    logger.info("Preparing input")
    val keyedRatings = r.flatMap(r => Seq(((USER_KEY, r.user), r), ((ITEM_KEY, r.item), r))).materialize
    val vectors = r
      .flatMap { r => Seq((USER_KEY, r.user), (ITEM_KEY, r.item)) }
      .distinct()
      .map((_, DenseVector.rand[Double](rank)))
      .materialize
    sc.close()
    (keyedRatings, vectors)
  }

  def run(): MatrixFactorizationModel = {
    val data = runIteration(1, prepareInput())
    val sc = ScioContext(options)
    val v = Await.result(data._2, Duration.Inf).open(sc)
    val Seq(u, i) = v
      .partition(2, x => if (x._1._1 == USER_KEY) 0 else 1)
      .map(_.map(kv => (kv._1._2, kv._2)).materialize)
    sc.close()
    MatrixFactorizationModel(rank, u, i)
  }

}
