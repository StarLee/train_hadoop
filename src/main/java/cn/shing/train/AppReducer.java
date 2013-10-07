package cn.shing.train;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AppReducer extends Reducer<Text, LongWritable, Text, Text> 
{

	protected void reduce(Text key, Iterable<LongWritable> values,Context context)
			throws IOException, InterruptedException {
		int i=0;
		for(LongWritable value:values)
		{
			i++;
		}
		context.write(key, new Text(String.valueOf(i)));
	}

}
