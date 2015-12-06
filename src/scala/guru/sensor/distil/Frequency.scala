package guru.sensor.distil

import io.btrdb._
import io.btrdb.distil._
import io.btrdb.distil.dsl._
import scala.collection.mutable

class Frequency extends Distiller {
  val version : Int
    = 1
  val maintainer : String
    = "Michael Andersen"
  val outputNames : Seq[String]
    = List("freq_1s", "freq_c37")
  val inputNames : Seq[String]
    = List("angle")
  val reqParams : Seq[String]
    = List()
  def kernelSizeNanos : Option[Long]
    = Some(1.second + 500.millisecond)
  val timeBaseAlignment : Option[BTrDBAlignMethod]
    = Some(BTRDB_ALIGN_120HZ_SNAP_DENSE)
  val dropNaNs : Boolean
    = false

  def angwrap(d:Double) : Double = {
    if (d > 180) d-360
    else if (d < -180) d +360
    else d
  }
  override def kernel(range : (Long, Long),
               rangeStartIdx : Int,
               input : IndexedSeq[(Long, IndexedSeq[Double])],
               inputMap : Map[String, Int],
               output : Map[String, mutable.ArrayBuffer[(Long, Double)]],
               db : BTrDB) = {

    //Our inputs are dense, so we can just use indexes. we will use 120 indexes
    println("rangestartidx is: ", rangeStartIdx)
    println("range is: ", range)
    println("input0 is: ", input(0))
    val out1s = output("freq_1s")
    val outc37 = output("freq_c37")
    for (i <- rangeStartIdx until input.size) {
      val time = input(i)._1

      val v1s = angwrap(input(i)._2(0) - input(i-120)._2(0))/360.0 + 60
      if (!v1s.isNaN)
        out1s += ((time, v1s))

      val p1 = input(i)._2(0)
      val p2 = input(i-1)._2(0)
      val p3 = input(i-2)._2(0)
      val p4 = input(i-3)._2(0)
      val v1 = angwrap(p1-p2)
      val v2 = angwrap(p2-p3)
      val v3 = angwrap(p3-p4)
      val c37 = 60.0 + (((6.0*(v1)+3.0*(v2)+1.0*(v3))/10)*((120.0/360.0)))
      if (!c37.isNaN)
        outc37 += ((time, c37))
    }
    deleteAllRanges(range)(db)
  }
}
