package sparkengine.scala.scripting

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.reflect.runtime.universe
import scala.tools.reflect.{ToolBox, ToolBoxError}


object ScriptEngine {

  val scriptCache = new ConcurrentHashMap[String, Any]();
  val environments = new ConcurrentHashMap[String, Any]();

  @throws[ToolBoxError]
  def evaluate(src: String, cache: Boolean, context: Option[Any] = None, contextClassName: Option[String] = None): Any = {
    if (cache)
      scriptCache.computeIfAbsent(src, code => evalCode(code, context, contextClassName))
    else
      evalCode(src, context, contextClassName)
  }

  private def evalCode(code: String, context: Option[Any], contextClassName: Option[String]): Any = {
    val toolBox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    val contextId = UUID.randomUUID().toString;
    try {
      val enrichedCode = enrichCode(contextId, code, context, contextClassName)
      val parsedCode = toolBox.parse(enrichedCode)
      toolBox.eval(parsedCode)
    } finally {
      environments.remove(contextId)
    }
  }

  private def enrichCode(contextId: String, src: String, context: Option[Any], contextClassName: Option[String]): String = {
    val contextId = UUID.randomUUID().toString;
    context match {
      case Some(contextValue) => {
        environments.put(contextId, contextValue)
        val contextClass = contextClassName match {
          case Some(name) => name
          case None => contextValue.getClass.getName
        }
        s"""
        val ctx = sparkengine.scala.scripting.ScriptEngine.environments.get("$contextId").asInstanceOf[$contextClass]
        $src
        """
      }
      case None => src
    }
  }

}
