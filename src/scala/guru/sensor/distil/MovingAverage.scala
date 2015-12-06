package guru.sensor.distil

import io.btrdb._
import io.btrdb.distil._
import io.btrdb.distil.dsl._
import scala.collection.mutable

class MovingAverageDistiller extends Distiller {
  val version : Int
    = 1
  val maintainer : String
    = "Michael Andersen"
  val outputNames : Seq[String]
    = List("avg")
  val inputNames : Seq[String]
    = List("input")
  val reqParams : Seq[String]
    = List("windowSeconds")
  def kernelSizeNanos : Option[Long]
    = Some(params("windowSeconds").toLong.second + 500.millisecond)
  val timeBaseAlignment : Option[BTrDBAlignMethod]
    = Some(BTRDB_ALIGN_120HZ_SNAP_DENSE)
  val dropNaNs : Boolean
    = false

  lazy val ptsInWindow : Int = params("windowSeconds").toInt * 120

  override def kernel(range : (Long, Long),
               rangeStartIdx : Int,
               input : IndexedSeq[(Long, IndexedSeq[Double])],
               inputMap : Map[String, Int],
               output : Map[String, mutable.ArrayBuffer[(Long, Double)]],
               db : BTrDB) = {

    //Our inputs are dense, so we can just use indexes. we will use 120 indexes
    val out = output("avg")
    //println(s"kernel started with input sz: ${input.size} and rangestartidx=${rangeStartIdx}")
    if (rangeStartIdx > 0) {

    }
    //Bootstrap
    var sum = input.view
      .slice(rangeStartIdx-(ptsInWindow+1), rangeStartIdx-1)
      .map(x=>x._2(0))
      .filterNot(_.isNaN)
      .foldLeft(0.0)(_+_)

    var count = input.view
      .slice(rangeStartIdx-(ptsInWindow+1), rangeStartIdx-1)
      .map(x=>x._2(0))
      .filterNot(_.isNaN)
      .size

    for (i <- rangeStartIdx until input.size) {
      if (!input(i-(ptsInWindow+1))._2(0).isNaN) {
        count -= 1
        sum -= input(i-(ptsInWindow+1))._2(0)
      }

      if (!input(i)._2(0).isNaN) {
        count += 1
        sum += input(i)._2(0)
      }

      if (count > 0)
        out += ((input(i)._1 , sum/count))
    }
    deleteAllRanges(range)(db)
  }
}
