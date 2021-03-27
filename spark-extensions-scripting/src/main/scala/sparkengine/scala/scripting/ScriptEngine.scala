package sparkengine.scala.scripting

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.reflect.runtime.universe
import scala.tools.reflect.{ToolBox, ToolBoxError}


object ScriptEngine {

  val scriptCache = new ConcurrentHashMap[String, Any]();
  val environments = new ConcurrentHashMap[String, Any]();

  @throws[ToolBoxError]
  def evaluate(src: String, context: Option[Any] = None, contextClassName: Option[String] = None): Any = {
    val toolBox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    scriptCache.computeIfAbsent(src, code => {

      val contextId = UUID.randomUUID().toString;
      try {
        val parsedCode = context match {
          case Some(contextValue) => {
            environments.put(contextId, contextValue)
            val contextClass = contextClassName match {
              case Some(name) => name
              case None => contextValue.getClass.getName
            }
            toolBox.parse(
              s"""
                val ctx = sparkengine.scala.scripting.ScriptEngine.environments.get("$contextId").asInstanceOf[$contextClass]
                $code
              """)
          }
          case None => toolBox.parse(code)
        }

        toolBox.eval(parsedCode)
      } finally {
        environments.remove(contextId)
      }
    })
  }

}
