package vanillaMR

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Reducer, Job, Mapper}
import org.apache.hadoop.conf.{Configured}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import scala.collection.JavaConversions._
import org.apache.hadoop.util.{ToolRunner, Tool}
import java.net._


object HostCount extends Configured with Tool {
  def run(args: Array[String]): Int = {

    val conf = getConf

    val job = new Job(conf, "Host Count")

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[HostCountMapper])
    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[IntWritable])

    job.setCombinerClass(classOf[HostCountReducer])
    job.setReducerClass(classOf[HostCountReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[IntWritable])

    FileInputFormat.addInputPath(job, new Path(args.head))
    FileOutputFormat.setOutputPath(job, new Path(args.last))

    if(job.waitForCompletion(true)) 0 else 1
  }

  def main(args: Array[String]) {
    System.exit(ToolRunner.run(this, Array("src/main/resources/sampleurls.txt","src/main/resources/hostcount")))
  }

  class HostCountMapper extends Mapper[IntWritable, Text, Text, IntWritable] {
    protected override def map(lnNumber: IntWritable, line: Text, context: Mapper[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      context.write(new Text(new URL(line.toString).getHost),new IntWritable(1))
    }
  }


  class HostCountReducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    protected override def reduce(key: Text, value: java.lang.Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val numURLsWithSameHost = new IntWritable(Integer.parseInt(value.reduceLeft(_ + _.toString)))
      if( numURLsWithSameHost.get() >= 5)
        context.write(key,numURLsWithSameHost)
    }
  }
}
