import akka.http.scaladsl.settings.ServerSettings
import com.bigdata.util.AppConfig
import com.bigdata.web.WebServer
import com.typesafe.config.ConfigFactory

/**
  * Created by sruthi on 03/07/17.
  */
object MainApp extends App {

  // init config params from cmd-line args
  AppConfig.parse(this.args.toList)

  // Starting the server
  WebServer.startServer("localhost", AppConfig.akkaHttpPort, ServerSettings(ConfigFactory.load))

  println(s"Server online at http://localhost:", AppConfig.akkaHttpPort, "/")
}
