package cn.shing.train;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class AppMain 
{
	public  static void main(String[] args) throws IOException, URISyntaxException, ClassNotFoundException, InterruptedException
	{
		Configuration conf=new Configuration();
		Job job=new Job(conf,"count word");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);
		job.setMapperClass(AppMapper.class);
		job.setReducerClass(AppReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setJarByClass(AppMain.class);
		FileSystem fs=FileSystem.get(conf);
		Path path_in=new Path("/train/text");
		Path path_out=new Path("/train/textout");
		if(!fs.exists(path_in))
		{
			URI uri=new URI("file:///home/starlee/Documents/text");
			Path path_local=new Path(uri);
			fs.copyFromLocalFile(path_local, path_in);
		}
		if(fs.exists(path_out))
		{
			fs.delete(path_out, true);
		}
		FileInputFormat.addInputPath(job, path_in);
		FileOutputFormat.setOutputPath(job, path_out);
		job.submit();
		while(true)
		{
			if(job.waitForCompletion(true))
				break;
		}
		
	}
}
