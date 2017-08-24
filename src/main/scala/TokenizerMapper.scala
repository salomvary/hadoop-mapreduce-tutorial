import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce._

/**
  * Hadoop Word Count mapper
  *
  * This mapper takes text as input value (ignores the key)
  * and breaks up the text into words. The output keys will
  * be the words, the value always 1 (one).
  */
class TokenizerMapper

  // A mapper has to inherit from the Mapper base class.
  // The type parameters are input key, input value, output key
  // output value.
  extends Mapper[IntWritable, Text, Text, IntWritable] {

  /**
    * The only public method to implement is map().
    *
    * @param key Ignored
    * @param value Text from the input files
    */
  override def map(key: IntWritable,
                   value: Text,
                   context: Mapper[IntWritable, Text, Text, IntWritable]#Context) {

    // Use split() as a rather naive way of turning text into words
    // Note: value.toString is necessary to unwrap Hadoop's Text class
    val words = value.toString.split("\\s+")

    // Output a key - value pair for each word
    words.foreach { word =>

      // The Hadoop framework operates on its own types therefore
      // everything needs to be wrapped, can not just use String or Int
      val key = new Text(word)
      val value = new IntWritable(1)

      // Use context.write() to "return" the wrapped key - value pairs
      context.write(key, value)
    }

  }
}