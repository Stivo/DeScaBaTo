package backup

import org.apache.ftpserver.FtpServerFactory
import org.apache.ftpserver.listener.ListenerFactory
import org.apache.ftpserver.usermanager.PropertiesUserManagerFactory
import org.apache.ftpserver.usermanager.impl.BaseUser
import org.apache.ftpserver.usermanager.SaltedPasswordEncryptor
import org.apache.ftpserver.usermanager.ClearTextPasswordEncryptor
import java.io.File
import java.util.ArrayList
import org.apache.ftpserver.ftplet.Authority
import org.apache.ftpserver.usermanager.impl.WritePermission
import org.apache.ftpserver.{FtpServer => JFtpServer}
object FtpServer {

  private var counter = 2221
  private var servers = Map[Int, JFtpServer]()
  def server = synchronized {
	  val port = counter
	  counter += 1
	  lazy val serverFactory = new FtpServerFactory();
	  
	  lazy val userManagerFactory = new PropertiesUserManagerFactory();
	  serverFactory.setUserManager(userManagerFactory.createUserManager());
	  userManagerFactory.setPasswordEncryptor(new ClearTextPasswordEncryptor());
	  lazy val um = userManagerFactory.createUserManager();
	  lazy val user = new BaseUser();
	  user.setName("testvfs");
	  user.setPassword("asdfasdf");
	  val f = new File("testftp"+counter);
	  f.mkdir()
	  
	  user.setHomeDirectory(f.getAbsolutePath());
	  
	  val auths = new ArrayList[Authority]();
	  auths.add(new WritePermission());
	  user.setAuthorities(auths);
	  
	  um.save(user);
	  
	  lazy val factory = new ListenerFactory();
	  // 	set the port of the listener
	  factory.setPort(port);
	  // 	replace the default listener
	  serverFactory.addListener("default", factory.createListener());
	  // 	start the server
	  serverFactory.setUserManager(um)
	  lazy val server = serverFactory.createServer();
	  server.start();
	  servers += ((port, server))
	  port
  }
  
  def stop(i: Int) {
    servers(i).stop
    
  }
}