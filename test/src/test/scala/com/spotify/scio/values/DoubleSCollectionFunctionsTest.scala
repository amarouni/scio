package com.spotify.scio.values

import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.util.StatCounter

class DoubleSCollectionFunctionsTest extends PipelineSpec {

  val ints = 1 to 100
  val longs = (1 to 100).map(_.toLong)
  val floats = (1 to 100).map(_.toFloat)
  val doubles = (1 to 100).map(_.toDouble)
  val expected = StatCounter((1 to 100).map(_.toDouble): _*)

  def test(s: Seq[Double], e: Double): Unit = {
    s.size should equal (1L)
    s.head should equal (e +- 1e-10)
  }

  "DoubleSCollection" should "support sampleStdev()" in {
    val e = expected.sampleStdev
    test(runWithData(ints)(_.sampleStdev()), e)
    test(runWithData(longs)(_.sampleStdev()), e)
    test(runWithData(floats)(_.sampleStdev()), e)
    test(runWithData(doubles)(_.sampleStdev()), e)
  }

  it should "support sampleVariance()" in {
    val e = expected.sampleVariance
    test(runWithData(ints)(_.sampleVariance()), e)
    test(runWithData(longs)(_.sampleVariance()), e)
    test(runWithData(floats)(_.sampleVariance()), e)
    test(runWithData(doubles)(_.sampleVariance()), e)
  }

  it should "support stdev()" in {
    val e = expected.stdev
    test(runWithData(ints)(_.stdev()), e)
    test(runWithData(longs)(_.stdev()), e)
    test(runWithData(floats)(_.stdev()), e)
    test(runWithData(doubles)(_.stdev()), e)
  }

  it should "support variance()" in {
    val e = expected.variance
    test(runWithData(ints)(_.variance()), e)
    test(runWithData(longs)(_.variance()), e)
    test(runWithData(floats)(_.variance()), e)
    test(runWithData(doubles)(_.variance()), e)
  }

}