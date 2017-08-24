import java.lang.Iterable

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Reducer

import scala.collection.JavaConverters._

class IntSumReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
  private val result = new IntWritable

  override def reduce(key: Text,
                      values: Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context) {
    val sum = values.asScala.map(_.get()).sum
    result.set(sum)
    context.write(key, result)
  }
}