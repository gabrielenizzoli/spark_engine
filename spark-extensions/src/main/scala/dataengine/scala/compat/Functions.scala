package dataengine.scala.compat

import java.io.Serializable

class JavaToScalaFunction1[T, R](jf: java.util.function.Function[T, R]) extends scala.AnyRef
  with scala.Function1[T, R] with Serializable {

  def apply(x1: T): R = {
    jf.apply(x1);
  }

}

class JavaToScalaFunction2[T1, T2, R](jf: java.util.function.BiFunction[T1, T2, R]) extends scala.AnyRef
  with scala.Function2[T1, T2, R] with Serializable {

  def apply(x1: T1, x2: T2): R = {
    jf.apply(x1, x2);
  }

}

trait JavaUdfToScalaFunction extends Serializable

class JavaUdf0ToScalaFunction0[R](jf: org.apache.spark.sql.api.java.UDF0[R]) extends scala.AnyRef
  with scala.Function0[R] with JavaUdfToScalaFunction {

  def apply(): R = {
    jf.call();
  }

}

class JavaUdf1ToScalaFunction1[T1, R](jf: org.apache.spark.sql.api.java.UDF1[T1, R]) extends scala.AnyRef
  with scala.Function1[T1, R] with JavaUdfToScalaFunction {

  def apply(x1: T1): R = {
    jf.call(x1);
  }

}

class JavaUdf2ToScalaFunction2[T1, T2, R](jf: org.apache.spark.sql.api.java.UDF2[T1, T2, R]) extends scala.AnyRef
  with scala.Function2[T1, T2, R] with JavaUdfToScalaFunction {

  def apply(x1: T1, x2: T2): R = {
    jf.call(x1, x2);
  }

}