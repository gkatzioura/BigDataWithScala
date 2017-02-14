package com.gkatzioura.scala

import java.util.StringTokenizer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper}


/**
  * Created by gkatzioura on 2/14/17.
  */
package object WordCount {

  class TokenizerMapper extends Mapper[Object, Text, Text, IntWritable] {
    val one = new IntWritable(1)
    var word = new Text();
    def map(key: Object,value: Text,context: Context) = {
      val itr = new StringTokenizer(value.toString) ;
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
      }
    }
  }


  def main(args: Array[String]): Unit = {
    val configuration = Configuration
    val job = Job.getInstance(configuration,"word count")
    job.setJarByClass(WordCount.getClass)
//    job.setMapperClass()

//    job.setMapperClass(TokenizerMapper.class);
//    job.setCombinerClass(IntSumReducer.class);
//    job.setReducerClass(IntSumReducer.class);
//    job.setOutputKeyClass(Text.class);
//    job.setOutputValueClass(IntWritable.class);
//    FileInputFormat.addInputPath(job, new Path(args[0]));
//    FileOutputFormat.setOutputPath(job, new Path(args[1]));
//    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}
