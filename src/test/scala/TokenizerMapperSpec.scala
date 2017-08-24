import org.apache.hadoop.io._
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class TokenizerMapperSpec extends FlatSpec with MockitoSugar {

  "map" should "output the words split on spaces" in {
    // Instantiate the test subject (the mapper)
    val mapper = new TokenizerMapper

    // The MapReduce framework provides a Context instance in production.
    // In unit tests we will pass in a test double instead.
    val context = mock[mapper.Context]

    // Invoke the map() method with some test input and the context
    mapper.map(
      key = null,
      value = new Text("foo bar foo"),
      context
    )

    // As the return type of map() is Unit we need to check the
    // behavior by verifying side effects (method calls) on context.

    // This verification ensures there were calls to context.write()
    // with the given arguments (the words and 1).
    verify(context, times(2)).write(new Text("foo"), new IntWritable(1))
    verify(context).write(new Text("bar"), new IntWritable(1))
  }

}
