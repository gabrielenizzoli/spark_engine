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

class JavaUdf3ToScalaFunction3[T1, T2, T3, R](jf: org.apache.spark.sql.api.java.UDF3[T1, T2, T3, R]) extends scala.AnyRef
  with scala.Function3[T1, T2, T3, R] with JavaUdfToScalaFunction {

  def apply(x1: T1, x2: T2, x3: T3): R = {
    jf.call(x1, x2, x3);
  }

}

class JavaUdf4ToScalaFunction4[T1, T2, T3, T4, R](jf: org.apache.spark.sql.api.java.UDF4[T1, T2, T3, T4, R]) extends scala.AnyRef
  with scala.Function4[T1, T2, T3, T4, R] with JavaUdfToScalaFunction {

  def apply(x1: T1, x2: T2, x3: T3, x4: T4): R = {
    jf.call(x1, x2, x3, x4);
  }

}

class JavaUdf5ToScalaFunction5[T1, T2, T3, T4, T5, R](jf: org.apache.spark.sql.api.java.UDF5[T1, T2, T3, T4, T5, R]) extends scala.AnyRef
  with scala.Function5[T1, T2, T3, T4, T5, R] with JavaUdfToScalaFunction {

  def apply(x1: T1, x2: T2, x3: T3, x4: T4, x5: T5): R = {
    jf.call(x1, x2, x3, x4, x5);
  }

}