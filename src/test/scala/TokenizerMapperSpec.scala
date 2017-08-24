import org.apache.hadoop.io.{IntWritable, Text}
import org.mockito.Mockito._
import org.scalatest._
import org.scalatest.mockito.MockitoSugar

class TokenizerMapperSpec extends FlatSpec with Matchers with MockitoSugar {
  "map" should "return the words split on spaces" in {
    val mapper = new TokenizerMapper
    val context = mock[mapper.Context]

    mapper.map(
      key = null,
      value = new Text("foo"),
      context
    )

    // Would be nice to test with more than one write() but Mockito
    // chokes on it with java.lang.VerifyError: Bad return type
    verify(context).write(new Text("foo"), new IntWritable(1))
  }
}
