package com.aamend.spark.gdelt

import org.scalatest.{FlatSpec, Matchers}

class TTest extends FlatSpec with Matchers {

  "null" should "return None" in {
    T(()=>null) should be(None)
    T(()=>null.toString) should be(None)
  }

  "Integer" should "return Int" in {
    T(()=>"1".toInt) should be(Some(1))
    T(()=>"a".toInt) should be(None)
  }

  "Long" should "return Long" in {
    T(()=>"1".toLong) should be(Some(1L))
    T(()=>"a".toLong) should be(None)
  }

  "Float" should "return Float" in {
    T(()=>"1.0".toFloat) should be(Some(1.0))
    T(()=>"a".toFloat) should be(None)
  }

  "String" should "return String" in {
    T(()=>"1") should be(Some("1"))
    T(()=>" 1 ") should be(Some("1"))
    T(()=>" ") should be(None)
    T(()=>"") should be(None)
  }

}
