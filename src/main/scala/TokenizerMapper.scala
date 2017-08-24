import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

class TokenizerMapper extends Mapper[IntWritable, Text, Text, IntWritable] {
  private val one = new IntWritable(1)
  private val word = new Text

  override def map(key: IntWritable,
                   value: Text,
                   context: Mapper[IntWritable, Text, Text, IntWritable]#Context) {
    value.toString.split("\\s+").foreach { nextWord =>
      word.set(nextWord)
      context.write(word, one)
    }
  }
}