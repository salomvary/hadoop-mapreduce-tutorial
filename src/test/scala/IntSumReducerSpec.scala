import org.apache.hadoop.io.{IntWritable, Text}
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

import scala.collection.JavaConverters._

class IntSumReducerSpec extends FlatSpec with Matchers with MockitoSugar {
  "reduce" should "sum the values" in {
    val reducer = new IntSumReducer
    val context = mock[reducer.Context]

    reducer.reduce(
      key = new Text("one"),
      values = Seq(new IntWritable(1), new IntWritable(1)).asJava,
      context
    )

    verify(context).write(new Text("one"), new IntWritable(2))
  }
}
