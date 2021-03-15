package sparkengine.scala.scripting

import org.apache.spark.sql.expressions.UserDefinedFunction

import java.util.concurrent.ConcurrentHashMap
import scala.reflect.runtime.universe
import scala.tools.reflect.ToolBox

object ScriptEngine {

  val cache = new ConcurrentHashMap[String, Any]();

  def compileToUserDefinedFunction(code: String): UserDefinedFunction = {
    val toolBox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val src =
      s"""
        | org.apache.spark.sql.functions.udf({
        |   ${code}
        | })
        | """.stripMargin

    val outcome = toolBox.eval(toolBox.parse(src))
    outcome.asInstanceOf[UserDefinedFunction]
  }

  def compileToFunction(src: String): Any = {
    val toolBox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    cache.computeIfAbsent(src, s => toolBox.eval(toolBox.parse(src)))
  }

}
