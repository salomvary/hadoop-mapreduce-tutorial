import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

object WordCount extends App {
  // An instance of Configuration is required for setting up a Job
  val conf = new Configuration
  // A Job allows gluing inputs, mapper, reducer and outputs together
  val job = Job.getInstance(conf, "Word Count")
  // Mandatory magic for running jar-packaged jobs on a cluster
  job.setJarByClass(classOf[TokenizerMapper])

  // Input files are expected to be under a folder named "input"
  // relative to the current directory
  FileInputFormat.addInputPath(job, new Path("input"))

  // This is where we wire in the mapper
  job.setMapperClass(classOf[TokenizerMapper])
  // This is where we wire in the reducer
  job.setReducerClass(classOf[IntSumReducer])

  // The key and value type for the output file must be specified
  job.setOutputKeyClass(classOf[Text])
  job.setOutputValueClass(classOf[IntWritable])

  // Output file(s) are going to be created under a folder named "output"
  // relative to the current directory
  FileOutputFormat.setOutputPath(job, new Path("output"))

  // Run the job and wait until it succeeds or fails
  job.waitForCompletion(true)
}