import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest._

import scala.io.Source

class WordCountIntegrationSpec
  extends FlatSpec
    with Matchers
    with BeforeAndAfterEach {

  "WordCount" should "write out word counts to output folder" in {
    // Run the MapReduce job locally
    WordCount.main(Array())

    // Read the entire output into a string
    val output = Source.fromFile("output/part-r-00000").mkString

    // Verify the output is what we expected
    output should equal(
      """|Bye	1
         |Goodbye	1
         |Hadoop	2
         |Hello	2
         |World	2
         |""".stripMargin)
  }

  override def afterEach() = {
    // Make sure the integration tests leave no files behind
    // by recursively deleting the output folder after each test run.
    // IMPORTANT: integration tests in a real life project should
    // not write files under the project root folder, they should use
    // temporary folders provided by the operating system (eg. under /tmp).
    val fs = FileSystem.get(new Configuration())
    fs.delete(new Path("output"), true)
  }
}
