import org.apache.hadoop.io._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class IntSumReducerSpec extends FlatSpec with MockitoSugar {

  "reduce" should "output the word and the sum of its occurrences" in {
    // Instantiate the test subject (the reducer)
    val reducer = new IntSumReducer

    // The MapReduce framework provides a Context instance in production.
    // In unit tests we will pass in a test double instead.
    val context = mock[reducer.Context]

    // Invoke the reduce() method with some test input and the context
    reducer.reduce(
      key = new Text("one"),
      // The MapReduce framework requires a Java Iterable which we
      // create from a Scala Seq with .asJava
      values = Seq(new IntWritable(1), new IntWritable(1)).asJava,
      context
    )

    // As the return type of reduce() is Unit we need to check the
    // behavior by verifying side effects (method calls) on context.

    // This verification ensures there was a call to context.write()
    // with the given arguments (the word and the sum of its occurrences).
    verify(context).write(new Text("one"), new IntWritable(2))
  }

}
