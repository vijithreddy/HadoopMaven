package org.mapreduce.drivers;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.mapreduce.mappers.WordCountMapper;
import org.mapreduce.reducers.WordCountReducer;


public class WordCountDriver extends Configured implements Tool{
	public static void main( String[] args) throws  Exception {
	     int res  = ToolRunner.run( new WordCountDriver(), args);
	     System .exit(res);
	  }
	public int run(String[] arg) throws Exception {
		Job job  = Job.getInstance(getConf(), " wordcount ");
		job.setJarByClass( this .getClass());
		FileInputFormat.addInputPath(job,  new Path(arg[0]));
	    FileOutputFormat.setOutputPath(job,  new Path(arg[1]));
	    job.setMapperClass(WordCountMapper.class);
	    job.setCombinerClass(WordCountReducer.class);
	    job.setNumReduceTasks(2);
	    job.setReducerClass(WordCountReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    
		return job.waitForCompletion(true)?0:1;
	}
	

}
