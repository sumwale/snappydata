/*
 * Copyright (c) 2017-2019 TIBCO Software Inc. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package io.snappydata.cluster

import java.io._
import java.nio.file.{Files, Paths}
import java.sql.{Connection, DriverManager, ResultSet, SQLException, Statement}
import java.util

import scala.language.postfixOps
import scala.sys.process._
import io.snappydata.Constant
import io.snappydata.test.dunit.{AvailablePortHelper, DistributedTestBase}
import org.apache.commons.io.FileUtils
import org.apache.commons.io.filefilter.{IOFileFilter, TrueFileFilter, WildcardFileFilter}
import org.apache.spark.Logging

class DeployUndeployDUnitTest(val s: String)
    extends DistributedTestBase(s) with SnappyJobTestSupport with Logging {
  // scalastyle:off println

  def getConnection(netPort: Int): Connection =
    DriverManager.getConnection(s"${Constant.DEFAULT_THIN_CLIENT_URL}localhost:$netPort")

  override val snappyProductDir = System.getenv("SNAPPY_HOME")

  val cassandraScriptPath = s"$snappyProductDir/../../../cluster/src/test/resources/scripts"
  val mongodbJsonDataPath = s"$snappyProductDir/../../../spark/examples/src/main/resources"
  val downloadPath = s"$snappyProductDir/../../../dist"

  lazy val downloadLoc = {
    val path = if (System.getenv().containsKey("GRADLE_USER_HOME")) {
      Paths.get(System.getenv("GRADLE_USER_HOME"), "cassandraDist")
    } else {
      Paths.get(System.getenv("HOME"), ".gradle", "cassandraDist")
    }
    Files.createDirectories(path)
    path.toString
  }

  val userHome = System.getProperty("user.home")

  val currDir = System.getProperty("user.dir")

  var clusterLoc = ""
  var jarLoc: List[String] = List.empty
  var connectorJarLoc = ""
  var jarUrl = ""
  var connectorJarUrl = ""

  var cassandraClusterLoc = ""
  var cassandraConnectorJarLoc = ""

  var mongodbClusterLoc = ""
  var mongodbConnectorJarLoc = ""

  var sparkXmlJarPath = ""

  private val commandOutput = "command-output.txt"

  val port = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort = AvailablePortHelper.getRandomAvailableTCPPort
  val netPort2 = AvailablePortHelper.getRandomAvailableTCPPort

  def snappyShell: String = s"$snappyProductDir/bin/snappy-sql"

  override def beforeClass(): Unit = {

    super.beforeClass()

    // start snappy clsuter
    logInfo(s"Starting snappy cluster in $snappyProductDir/work with locator client port $netPort")

    val confDir = s"$snappyProductDir/conf"
    val sobj = new SplitClusterDUnitTest(s)
    sobj.writeToFile(s"localhost  -peer-discovery-port=$port -client-port=$netPort",
      s"$confDir/locators")
    sobj.writeToFile(s"localhost  -locators=localhost[$port]",
      s"$confDir/leads")
    sobj.writeToFile(s"""localhost  -locators=localhost[$port] -client-port=$netPort2
                        |""".stripMargin, s"$confDir/servers")
    logInfo(s"Starting snappy cluster in $snappyProductDir/work")

    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh").!!)
    Thread.sleep(10000)
    logInfo("Download Location : " + downloadLoc)

    logInfo(s"Creating $downloadPath")
    new File(downloadPath).mkdir()
    new File(snappyProductDir, "books.xml").createNewFile()
    sparkXmlJarPath = downloadURI("https://repo1.maven.org/maven2/com/databricks/" +
        "spark-xml_2.11/0.5.0/spark-xml_2.11-0.5.0.jar")

    // start cassandra cluster
    jarLoc = getLoc(downloadLoc, "apache-cassandra-2.1.21")
    connectorJarLoc =
        getUserAppJarLocation("spark-cassandra-connector_2.11-2.0.7.jar", downloadLoc)
    jarUrl = "http://www-us.apache.org/dist/cassandra/2.1.21/apache-cassandra-2.1.21-bin.tar.gz"
    connectorJarUrl = "https://repo1.maven.org/maven2/com/datastax/spark/" +
        "spark-cassandra-connector_2.11/2.0.7/spark-cassandra-connector_2.11-2.0.7.jar"
    startCluster("apache-cassandra-2.1.21-bin.tar.gz",
      "spark-cassandra-connector_2.11-2.0.7.jar",
      jarUrl, connectorJarUrl, "apache-cassandra-2.1.21")
    cassandraClusterLoc = clusterLoc
    cassandraConnectorJarLoc = connectorJarLoc
    startCassandraCluster()

    // start mongodb cluster
    jarLoc = getLoc(downloadLoc, "mongodb-linux-x86_64-ubuntu1604-4.2.0")
    connectorJarLoc =
        getUserAppJarLocation("mongo-spark-connector_2.11-2.1.6.jar", downloadLoc)
    jarUrl = "https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-ubuntu1604-4.2.0.tgz"
    connectorJarUrl = "https://repo1.maven.org/maven2/org/mongodb/spark/" +
        "mongo-spark-connector_2.11/2.1.6/mongo-spark-connector_2.11-2.1.6.jar"
    startCluster("mongodb-linux-x86_64-ubuntu1604-4.2.0.tgz",
      "mongo-spark-connector_2.11-2.1.6.jar",
      jarUrl, connectorJarUrl, "mongodb-linux-x86_64-ubuntu1604-4.2.0")
    mongodbClusterLoc = clusterLoc
    mongodbConnectorJarLoc = connectorJarLoc
    startMongodbCluster()
  }

  override def afterClass(): Unit = {
    super.afterClass()

    // stop mongodb cluster
    val cmd1 =
      s"""./mongo --port=23456 employeesDB $cassandraScriptPath/mongodb_script1""".stripMargin
    println(mongodbClusterLoc + cmd1)
    val p2 = Process(cmd1, new File(mongodbClusterLoc + "/bin")).lineStream_!
    println(p2.print())
    logInfo("Stopping mongodb cluster")
    val p1 = Runtime.getRuntime.exec("pkill -f mongo")
    p1.waitFor()
    p1.exitValue() == 0
    logInfo("MongoDb cluster stopped successfully")

    // stop cassandra cluster
    logInfo("Stopping cassandra cluster")
    val p = Runtime.getRuntime.exec("pkill -f cassandra")
    p.waitFor()
    p.exitValue() == 0
    logInfo("Cassandra cluster stopped successfully")

    // stop snappy cluster
    logInfo(s"Stopping snappy cluster in $snappyProductDir/work")
    logInfo((snappyProductDir + "/sbin/snappy-stop-all.sh").!!)

    // s"rm -rf $snappyProductDir/work".!!
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "locators"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "leads"))
    Files.deleteIfExists(Paths.get(snappyProductDir, "conf", "servers"))
  }

  def startCluster(jarName: String, connectorJarName: String,
                   jarUrl: String, connectorJarUrl: String, clusterFolderName: String): Unit = {
    if (jarLoc.nonEmpty && connectorJarLoc != null) {
      clusterLoc = jarLoc.head
    } else {
      (s"curl -OL $jarUrl").!!
      (s"curl -OL $connectorJarUrl").!!
      val tempJarLoc = getUserAppJarLocation(jarName, currDir)
      val tempConnectorJarLoc =
        getUserAppJarLocation(connectorJarName, currDir)
      ("tar xvf " + tempJarLoc).!!
      val loc = getLoc(currDir, clusterFolderName).head
      if (downloadLoc.nonEmpty) {
        s"rm -rf $downloadLoc/$clusterFolderName" +
            s"$downloadLoc/$connectorJarName"
      }
      s"cp -r $loc $downloadLoc".!!
      s"mv $tempConnectorJarLoc $downloadLoc".!!
      clusterLoc = s"$downloadLoc/$clusterFolderName"
      connectorJarLoc = s"$downloadLoc/$connectorJarName"
    }
    logInfo(s"${clusterFolderName} Cluster Location : " + clusterLoc +
        " ConnectorJarLoc : " + connectorJarLoc)
  }

  def startCassandraCluster(): Unit = {
    (cassandraClusterLoc + "/bin/cassandra").!!
    (cassandraClusterLoc + s"/bin/cqlsh -f $cassandraScriptPath/cassandra_script1").!!
    logInfo("Cassandra cluster started")
  }

  def startMongodbCluster(): Unit = {
    var dataDBPath = ""
    dataDBPath = mongodbClusterLoc + "/data/db"
    s"mkdir -p $dataDBPath".!!
    val p = Process(s"./mongod --dbpath=$dataDBPath " +
        s"--port=23456 --fork --logpath /var/log/mongodb.log",
      new File(mongodbClusterLoc + "/bin")).lineStream_!
    val cmd =
      s"""./mongoimport --port 23456 --db employeesDB --collection employees
         | --type json --file $mongodbJsonDataPath/employees.json""".stripMargin
    val p1 = Process(cmd, new File(mongodbClusterLoc + "/bin")).lineStream_!
    logInfo("Mongodb cluster started")
  }

  def getLoc(path: String, dataSourceFolderName: String): List[String] = {
    val cmd = Seq("find", path, "-name", dataSourceFolderName, "-type", "d")
    val res = cmd.lineStream_!.toList
    logInfo(s"${dataSourceFolderName} Folder location : " + res)
    res
  }

  private def downloadURI(url: String): String = {
    val jarName = url.split("/").last
    val jar = new File(downloadPath, jarName)
    if (!jar.exists()) {
      logInfo(s"Downloading $url ...")
      s"curl -OL $url".!!
      val cmd = s"find $currDir -name $jarName"
      logInfo(s"Executing $cmd")
      val tempPath = cmd.lineStream_!.toList
      val tempJar = new File(tempPath.head)
      assert(tempJar.exists(), s"Did not find $jarName at $tempPath")
      assert(tempJar.renameTo(jar), s"Could not move $jarName to $downloadPath")
    }
    jar.getAbsolutePath
  }

  protected def getUserAppJarLocation(jarName: String, jarPath: String) = {
    var userAppJarPath: String = null
    if (new File(jarName).exists) jarName
    else {
      val baseDir: File = new File(jarPath)
      try {
        val filter: IOFileFilter = new WildcardFileFilter(jarName)
        val files: util.List[File] = FileUtils.listFiles(baseDir, filter,
          TrueFileFilter.INSTANCE).asInstanceOf[util.List[File]]
        logInfo("Jar file found: " + util.Arrays.asList(files))
        import scala.collection.JavaConverters._
        for (file1: File <- files.asScala) {
          if (!file1.getAbsolutePath.contains("/work/") ||
              !file1.getAbsolutePath.contains("/scala-2.11/")) {
            userAppJarPath = file1.getAbsolutePath
          }
        }
      }
      catch {
        case _: Exception =>
          logInfo("Unable to find " + jarName + " jar at " + jarPath + " location.")
      }
      userAppJarPath
    }
  }

  implicit class X(in: Seq[String]) {
    def pipe(cmd: String): Stream[String] =
      cmd #< new ByteArrayInputStream(in.mkString("\n").getBytes) lineStream
  }

  def SnappyShell(name: String, sqlCommand: Seq[String]): Unit = {
    val writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(commandOutput, true))))
    try {
      sqlCommand pipe snappyShell foreach (s => {
        writer.println(s)
        if (s.toString.contains("ERROR") || s.toString.contains("Failed")) {
          throw new Exception(s"Failed to run Query: $s")
        }
      })
    } finally {
      writer.close()
    }
  }

  def getCount(rs: ResultSet): Int = {
    var count = 0
    if (rs ne null) {
      while (rs.next()) {
        count += 1
      }
      rs.close()
    }
    count
  }

  private var user1Conn: Connection = null
  private var stmt1: Statement = null

  def testDeployPackageWithCassandra(): Unit = {
    user1Conn = getConnection(netPort)
    stmt1 = user1Conn.createStatement()
    doTestPackageViaSnappyJobCommandWithCassandraDS()
    doTestDeployPackageWithExternalTableWithCassandraDS()
    doTestDeployJarWithExternalTableWithCassandraDS()
    doTestDeployJarWithSnappyJobWithCassandraDS()
    doTestDeployPackageWithSnappyJobWithCassandraDS()
    doTestDeployPackageWithExternalTableInSnappyShellWithCassandraDS()
    doTestPackageViaSnappyJobCommandWithMongodbDS()
  }

  def doTestPackageViaSnappyJobCommandWithCassandraDS(): Unit = {
    logInfo("Running testPackageViaSnappyJobCommand")
    submitAndWaitForCompletion("io.snappydata.cluster.jobs.CassandraSnappyConnectionJob" ,
      "--packages com.datastax.spark:spark-cassandra-connector_2.11:2.4.1" +
          " --conf spark.cassandra.connection.host=localhost")
  }

  def doTestDeployPackageWithExternalTableInSnappyShellWithCassandraDS(): Unit = {
    logInfo("Running testDeployPackageWithExternalTableInSnappyShell")
    SnappyShell("CreateExternalTable",
      Seq(s"connect client 'localhost:$netPort';",
        "deploy package cassandraJar 'com.datastax.spark:spark-cassandra-connector_2.11:2.0.7';",
        "drop table if exists customer2;",
        "create external table customer2 using org.apache.spark.sql.cassandra" +
            " options (table 'customer', keyspace 'test'," +
            " spark.cassandra.input.fetch.size_in_rows '200000'," +
            " spark.cassandra.read.timeout_ms '10000');",
        "select * from customer2;",
        "undeploy cassandraJar;",
        "exit;"))
  }

  def doTestDeployPackageWithExternalTableWithCassandraDS(): Unit = {
    logInfo("Running testDeployPackageWithExternalTable")
    stmt1.execute("deploy package cassandraJar " +
        "'com.datastax.spark:spark-cassandra-connector_2.11:2.0.7'")
    stmt1.execute("drop table if exists customer2")
    stmt1.execute("create external table customer2 using org.apache.spark.sql.cassandra options" +
        " (table 'customer', keyspace 'test', spark.cassandra.input.fetch.size_in_rows '200000'," +
        " spark.cassandra.read.timeout_ms '10000')")
    stmt1.execute("select * from customer2")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 1)

    stmt1.execute("undeploy cassandrajar")

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)

    stmt1.execute("drop table if exists customer2")
    try {
      stmt1.execute("create external table customer2 using org.apache.spark.sql.cassandra options" +
          " (table 'customer', keyspace 'test', " +
          "spark.cassandra.input.fetch.size_in_rows '200000'," +
          " spark.cassandra.read.timeout_ms '10000')")
      assert(assertion = false, s"Expected an exception!")
    } catch {
      case sqle: SQLException if (sqle.getSQLState == "42000") &&
          sqle.getMessage.contains("Failed to find " +
              "data source: org.apache.spark.sql.cassandra") => // expected
      case t: Throwable => assert(assertion = false, s"Unexpected exception $t")
    }
    stmt1.execute("deploy package cassandraJar " +
        "'com.datastax.spark:spark-cassandra-connector_2.11:2.0.7'")
    stmt1.execute("deploy package GoogleGSONAndAvro " +
        "'com.google.code.gson:gson:2.8.5,com.databricks:spark-avro_2.11:4.0.0' " +
        s"path '$snappyProductDir/testdeploypackagepath'")
    stmt1.execute("deploy package MSSQL 'com.microsoft.sqlserver:sqljdbc4:4.0'" +
        " repos 'http://clojars.org/repo/'")
    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 3)

    logInfo("Restarting the cluster for " +
        "CassandraSnappyDUnitTest.doTestDeployPackageWithExternalTable()")
    logInfo((snappyProductDir + "/sbin/snappy-stop-all.sh").!!)
    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh").!!)

    user1Conn = getConnection(netPort)
    stmt1 = user1Conn.createStatement()

    stmt1.execute("drop table if exists customer2")
    stmt1.execute("create external table customer2 using org.apache.spark.sql.cassandra options" +
        " (table 'customer', keyspace 'test', spark.cassandra.input.fetch.size_in_rows '200000'," +
        " spark.cassandra.read.timeout_ms '10000')")
    stmt1.execute("select * from customer2")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 3, s"After restart, packages expected 3," +
        s" found ${stmt1.getResultSet}")

    stmt1.execute("undeploy mssql")
    stmt1.execute("undeploy cassandrajar")
    stmt1.execute("undeploy googlegsonandavro")
    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)
  }

  def doTestDeployJarWithExternalTableWithCassandraDS(): Unit = {
    logInfo("Running testDeployJarWithExternalTable")
    stmt1.execute(s"deploy jar cassJar '$cassandraConnectorJarLoc'")
    stmt1.execute(s"deploy jar xmlJar '$sparkXmlJarPath'")
    stmt1.execute("drop table if exists customer3")
    stmt1.execute("create external table customer3 using org.apache.spark.sql.cassandra options" +
        " (table 'customer', keyspace 'test', spark.cassandra.input.fetch.size_in_rows '200000'," +
        " spark.cassandra.read.timeout_ms '10000')")
    stmt1.execute("select * from customer3")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 2)

    stmt1.execute("create external table books using com.databricks.spark.xml options" +
        s" (path '$snappyProductDir/books.xml')")

    // Move xml jar and verify the deploy fails upon restart.
    val xmlPath = new File(sparkXmlJarPath)
    val tempXmlPath = new File(s"$xmlPath.bak")
    assert(xmlPath.renameTo(tempXmlPath),
      s"Could not move ${xmlPath.getName} to ${tempXmlPath.getName}")

    logInfo("Restarting the cluster for " +
        "CassandraSnappyDUnitTest.doTestDeployJarWithExternalTable()")
    logInfo((snappyProductDir + "/sbin/snappy-stop-all.sh").!!)
    logInfo((snappyProductDir + "/sbin/snappy-start-all.sh").!!)

    user1Conn = getConnection(netPort)
    stmt1 = user1Conn.createStatement()
    stmt1.execute("drop table if exists customer3")

    stmt1.execute("create external table customer4 using org.apache.spark.sql.cassandra options" +
        " (table 'customer', keyspace 'test', spark.cassandra.input.fetch.size_in_rows '200000'," +
        " spark.cassandra.read.timeout_ms '10000')")
    stmt1.execute("select * from customer4")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 1)

    stmt1.execute("undeploy cassJar")
    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)

    assert(tempXmlPath.renameTo(xmlPath),
      s"Could not move ${tempXmlPath.getName} to ${xmlPath.getName}")

    stmt1.execute("drop table if exists customer4")
    try {
      stmt1.execute("create external table customer5 using org.apache.spark.sql.cassandra options" +
          " (table 'customer', keyspace 'test', " +
          "spark.cassandra.input.fetch.size_in_rows '200000'," +
          " spark.cassandra.read.timeout_ms '10000')")
      assert(assertion = false, s"Expected an exception!")
    } catch {
      case sqle: SQLException if (sqle.getSQLState == "42000") &&
          sqle.getMessage.contains("Failed to find " +
              "data source: org.apache.spark.sql.cassandra") => // expected
      case t: Throwable => assert(assertion = false, s"Unexpected exception $t")
    }

    try {
      stmt1.execute("create external table books2 using com.databricks.spark.xml options" +
          s" (path '$snappyProductDir/books.xml')")
      assert(false, "External table on xml should have failed.")
    } catch {
      case sqle: SQLException if (sqle.getSQLState == "42000") => // expected
      case t: Throwable => throw t
    }
  }

  def doTestDeployJarWithSnappyJobWithCassandraDS(): Unit = {
    logInfo("Running testDeployJarWithSnappyJob")
    stmt1.execute(s"deploy jar cassJar '$cassandraConnectorJarLoc'")
    stmt1.execute("drop table if exists customer")
    submitAndWaitForCompletion("io.snappydata.cluster.jobs.CassandraSnappyConnectionJob" ,
      "--conf spark.cassandra.connection.host=localhost")
    stmt1.execute("select * from customer")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 1)

    stmt1.execute("undeploy cassJar")

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)

    stmt1.execute("drop table if exists customer")
    try {
      submitAndWaitForCompletion("io.snappydata.cluster.jobs.CassandraSnappyConnectionJob" ,
        "--conf spark.cassandra.connection.host=localhost")
      assert(assertion = false, s"Expected an exception!")
    } catch {
      case e: Exception if e.getMessage.contains("Job failed with result:") => // expected
      case t: Throwable => assert(assertion = false, s"Unexpected exception $t")
    }
  }

  def doTestDeployPackageWithSnappyJobWithCassandraDS(): Unit = {
    logInfo("Running testDeployPackageWithSnappyJob")
    stmt1.execute("deploy package cassandraJar " +
        "'com.datastax.spark:spark-cassandra-connector_2.11:2.0.7'")
    stmt1.execute("drop table if exists customer")
    submitAndWaitForCompletion("io.snappydata.cluster.jobs.CassandraSnappyConnectionJob" ,
      "--conf spark.cassandra.connection.host=localhost")
    stmt1.execute("select * from customer")
    assert(getCount(stmt1.getResultSet) == 3)

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 1)
    stmt1.execute("undeploy cassandraJar")

    stmt1.execute("list packages")
    assert(getCount(stmt1.getResultSet) == 0)
    stmt1.execute("drop table if exists customer")
    try {
      submitAndWaitForCompletion("io.snappydata.cluster.jobs.CassandraSnappyConnectionJob" ,
        "--conf spark.cassandra.connection.host=localhost")
      assert(assertion = false, s"Expected an exception!")
    } catch {
      case e: Exception if e.getMessage.contains("Job failed with result:") => // expected
      case t: Throwable => assert(assertion = false, s"Unexpected exception $t")
    }
  }

  def doTestPackageViaSnappyJobCommandWithMongodbDS(): Unit = {
    logInfo("Running testPackageViaSnappyJobCommandWithMongodbDS")
    submitAndWaitForCompletion("io.snappydata.cluster.jobs.MongoDBSnappyConnectionJob" ,
      "--packages org.mongodb.spark:mongo-spark-connector_2.11:2.1.6")
  }
}
