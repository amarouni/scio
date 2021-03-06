/*
 * Copyright 2016 Spotify AB.
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

package com.spotify.scio.testing

import com.google.cloud.dataflow.sdk.Pipeline.PipelineExecutionException

class SCollectionMatcherTest extends PipelineSpec {

  "SCollectionMatch" should "support containInAnyOrder" in {
    runWithContext {
      _.parallelize(1 to 100) should containInAnyOrder (1 to 100)
    }

    intercept[AssertionError] {
      runWithContext { _.parallelize(1 to 200) should containInAnyOrder (1 to 100) }
    }
    intercept[AssertionError] {
      runWithContext { _.parallelize(1 to 100) should containInAnyOrder (1 to 200) }
    }
  }

  it should "support containSingleValue" in {
    runWithContext { _.parallelize(Seq(1)) should containSingleValue (1) }

    intercept[AssertionError] {
      runWithContext { _.parallelize(Seq(1)) should containSingleValue (10) }
    }
    intercept[PipelineExecutionException] {
      runWithContext { _.parallelize(1 to 10) should containSingleValue (1) }
    }
    intercept[PipelineExecutionException] {
      runWithContext { _.parallelize(Seq.empty[Int]) should containSingleValue (1) }
    }
  }

  it should "support beEmpty" in {
    runWithContext { _.parallelize(Seq.empty[Int]) should beEmpty }

    intercept[AssertionError] {
      runWithContext { _.parallelize(1 to 10) should beEmpty }
    }
  }

  it should "support haveSize" in {
    runWithContext { _.parallelize(Seq.empty[Int]) should haveSize (0) }
    runWithContext { _.parallelize(Seq(1)) should haveSize (1) }
    runWithContext { _.parallelize(1 to 10) should haveSize (10) }

    intercept[AssertionError] {
      runWithContext { _.parallelize(Seq.empty[Int]) should haveSize(1) }
    }
    intercept[AssertionError] {
      runWithContext { _.parallelize(Seq(1)) should haveSize(0) }
    }
    intercept[AssertionError] {
      runWithContext { _.parallelize(1 to 10) should haveSize (20) }
    }
  }

  it should "support equalMapOf" in {
    val s = Seq("a" -> 1, "b" -> 2, "c" -> 3)
    runWithContext { sc =>
      sc.parallelize(s) should equalMapOf (s.toMap)
      sc.parallelize(Seq.empty[(String, Int)]) should equalMapOf (Map.empty[String, Int])
    }

    intercept[AssertionError] {
      runWithContext { _.parallelize(s) should equalMapOf ((s :+ "d" -> 4).toMap) }
    }
    intercept[AssertionError] {
      runWithContext { _.parallelize(s :+ "d" -> 4) should equalMapOf (s.toMap) }
    }

    intercept[AssertionError] {
      runWithContext { _.parallelize(s) should equalMapOf (s.tail.toMap) }
    }
    intercept[AssertionError] {
      runWithContext { _.parallelize(s.tail) should equalMapOf (s.toMap) }
    }

    intercept[AssertionError] {
      runWithContext { _.parallelize(s) should equalMapOf (s.toMap + ("a" -> 10)) }
    }
    intercept[AssertionError] {
      runWithContext { _.parallelize(s.tail :+ ("a" -> 10)) should equalMapOf (s.toMap) }
    }

    intercept[AssertionError] {
      runWithContext { _.parallelize(s) should equalMapOf (Map.empty[String, Int]) }
    }
    intercept[AssertionError] {
      runWithContext { _.parallelize(Seq.empty[(String, Int)]) should equalMapOf (s.toMap) }
    }
  }

  it should "support forAll" in {
    runWithContext { _.parallelize(1 to 100) should forAll[Int] (_ > 0)}

    intercept[AssertionError] {
      runWithContext { _.parallelize(1 to 100) should forAll[Int] (_ > 10)}
    }
  }

  it should "support exist" in {
    runWithContext { _.parallelize(1 to 100) should exist[Int] (_ > 99)}

    intercept[AssertionError] {
      runWithContext { _.parallelize(1 to 100) should exist[Int] (_ > 100)}
    }
  }

}
