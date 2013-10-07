package cn.shing.train;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Hello world!
 *
 */
public class AppMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {
		StringTokenizer strToken=new StringTokenizer(value.toString());
		while(strToken.hasMoreTokens())
		{
			context.write(new Text(strToken.nextToken()), new LongWritable(1));
		}
	}
}
