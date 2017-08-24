import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._

class TokenizerMapper
  extends Mapper[IntWritable, Text, Text, IntWritable] {

  override def map(key: IntWritable,
                   value: Text,
                   context: Mapper[IntWritable, Text, Text, IntWritable]#Context) {

    val words = value.toString.split("\\s+")

    words.foreach { word =>
      val key = new Text(word)
      val value = new IntWritable(1)
      context.write(key, value)
    }

  }
}