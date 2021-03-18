package sparkengine.scala.scripting

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions.UserDefinedFunction

import java.util.concurrent.ConcurrentHashMap
import scala.reflect.runtime.universe
import scala.tools.reflect.{ToolBox, ToolBoxError}

object ScriptEngine {

  val cache = new ConcurrentHashMap[String, Any]();

  @throws[ToolBoxError]
  @throws[ClassCastException]
  def compileToUserDefinedFunction(code: String): UserDefinedFunction = {
    val src =
      s"""
         | org.apache.spark.sql.functions.udf({
         |   ${code}
         | })
         | """.stripMargin

    compileToObject(src).asInstanceOf[UserDefinedFunction]
  }

  @throws[ToolBoxError]
  def compileToObject(src: String): Any = {
    val toolBox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    cache.computeIfAbsent(src, s => toolBox.eval(toolBox.parse(src)))
  }

}
