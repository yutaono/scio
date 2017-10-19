import com.spotify.scio._
import com.spotify.scio.streaming._
import org.joda.time.{Duration, Instant}

// scalastyle:off
object Test {
  def main(args: Array[String]): Unit = {
    val (sc, _) = ContextAndArgs(Array("--streaming=true"))
    val now = Instant.now()
    val tsFn = (x: Long) => {
      val t = new Instant(now.plus(x * 1000))
      println("main", t)
      t
    }
    val oneSec = Duration.standardSeconds(1)

    val s1 = sc.sequence(0, 10, oneSec, timestampFn = tsFn).map("s1_" + _).withFixedWindows(oneSec)
    /*
    val s2 = sc.sequence(0, 10, oneSec, timestampFn = tsFn).withFixedWindows(oneSec)
    val side = s2
      .map { x =>
        val r = (1 to x.toInt).toList
        println(x, r)
        r
      }
      .asSingletonSideInput
      */
    val side = sc.refreshingSideInput(oneSec, _.toString())

    s1.withSideInputs(side)
      .map { case (x, s) =>
        val r = (x, s(side).mkString(", "))
        println("output", r)
        r
      }

    sc.close()
  }
}
