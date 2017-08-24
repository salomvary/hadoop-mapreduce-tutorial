import java.lang.Iterable

import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._

import scala.collection.JavaConverters._

/**
  * Hadoop Word Count Reducer
  *
  * This reducer takes a word as the key and as many 1 (one)
  * values as many times the word occurred. Outputs the word
  * and the total count of occurrences.
  */
class IntSumReducer

  // A reducer has to inherit from the Reducer base class.
  // The type parameters are input key, input value, output key
  // output value.
  extends Reducer[Text, IntWritable, Text, IntWritable] {

  /**
    * @param key A word
    * @param values As many 1s (ones) as many times the word occurred
    */
  override def reduce(key: Text,
                      values: Iterable[IntWritable],
                      context: Reducer[Text, IntWritable, Text, IntWritable]#Context) {

    val sum = values
      // Turn the Java Iterable into a Scala one
      // so that we can later use sum()
      .asScala
      // Unwrap the IntWritables into Ints
      .map(_.get())
      // Finally calculate the sum of the values
      .sum

    // The Hadoop framework operates on its own types therefore
    // everything needs to be wrapped, can not just use String or Int
    val value = new IntWritable(sum)

    // Use context.write() to "return" the wrapped key - value pairs
    context.write(key, value)
  }
}