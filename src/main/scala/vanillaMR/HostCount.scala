package vanillaMR

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
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
    job.setMapOutputValueClass(classOf[LongWritable])

    job.setCombinerClass(classOf[HostCountReducer])
    job.setReducerClass(classOf[HostCountReducer])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])

    FileInputFormat.addInputPath(job, new Path(args.head))
    FileOutputFormat.setOutputPath(job, new Path(args.last))

    if(job.waitForCompletion(true)) 0 else 1
  }

  class HostCountMapper extends Mapper[LongWritable, Text, Text, LongWritable] {
    protected override def map(lnNumber: LongWritable, line: Text, context: Mapper[LongWritable, Text, Text, LongWritable]#Context): Unit = {

      context.write(new Text(new URL(line.toString).getHost),new LongWritable(1))
    }
  }


  class HostCountReducer extends Reducer[Text, LongWritable, Text, LongWritable]{
    protected override def reduce(key: Text, value: java.lang.Iterable[LongWritable], context: Reducer[Text, LongWritable, Text, LongWritable]#Context): Unit = {

      val numURLsWithSameHost = value.map(_.get()).sum
      if( numURLsWithSameHost >= 5)
        context.write(key,new LongWritable(numURLsWithSameHost))

    }
  }

  def main(args: Array[String]) {
    System.exit(ToolRunner.run(this, Array("src/main/resources/sampleurls.txt","src/main/resources/hostcount")))
  }

}
