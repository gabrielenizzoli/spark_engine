package sparkengine.scala.scripting

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import scala.reflect.runtime.universe
import scala.tools.reflect.{ToolBox, ToolBoxError}


object ScriptEngine {

  val scriptCache = new ConcurrentHashMap[String, Any]();
  val environments = new ConcurrentHashMap[String, Any]();

  @throws[ToolBoxError]
  def evaluate(src: String, env: Option[Any] = None): Any = {
    val toolBox = universe.runtimeMirror(getClass.getClassLoader).mkToolBox()
    scriptCache.computeIfAbsent(src, code => {

      val envId = UUID.randomUUID().toString;
      val parsedCode = env match {
        case Some(envValue) => {
          environments.put(envId, envValue)
          toolBox.parse(
            s"""
            val env = sparkengine.scala.scripting.ScriptEngine.environments.get("$envId").asInstanceOf[${envValue.getClass.getName}]
            $code
          """)
        }
        case None => toolBox.parse(code)
      }

      val outcome = toolBox.eval(parsedCode)
      environments.remove(envId)
      return outcome
    })
  }

}
