import java.io.IOException;

import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class profile1 {
public static class TokenizerMapper extends Mapper<Object, Text, Text, NullWritable>{
private TreeMap <String, NullWritable> unimap = new TreeMap<String, NullWritable>();

public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	
if (!(value.toString().isEmpty())) {
	StringTokenizer itr = new StringTokenizer(value.toString().split("<====>")[2]);
	while (itr.hasMoreTokens()) {
	String out = itr.nextToken().replaceAll("[^A-Za-z0-9]","").toLowerCase();
	if(!out.isEmpty()) {
	unimap.put(out, null);
	}
	}
	if (unimap.size() > 500) 
    { 
		unimap.remove(unimap.lastKey()); 
    } 
}
}
public void cleanup(Context context) throws IOException, InterruptedException {
	for (String Key : unimap.keySet()) {
		context.write( new Text(Key), NullWritable.get());
	}
}
}
public static class IntSumReducer extends Reducer<Text,NullWritable, Text, NullWritable> {
private TreeMap <String, NullWritable> runimap = new TreeMap<String, NullWritable>();

public void reduce(Text key, Iterable<NullWritable> values,Context context) throws IOException, InterruptedException {
        
	runimap.put(key.toString(),null);
	if (runimap.size() > 500) 
    { 
		runimap.remove(runimap.lastKey()); 
    } 
}
public void cleanup(Context context) throws IOException, InterruptedException {
	for (String Key : runimap.keySet()) {
		context.write( new Text(Key), NullWritable.get());
	}
}
}
public static void main(String[] args) throws Exception {
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "ngram");
job.setJarByClass(profile1.class);
job.setMapperClass(TokenizerMapper.class);
job.setCombinerClass(IntSumReducer.class);
job.setReducerClass(IntSumReducer.class);
job.setOutputKeyClass(Text.class);
job.setOutputValueClass(NullWritable.class);
FileInputFormat.addInputPath(job, new Path(args[0]));
FileOutputFormat.setOutputPath(job, new Path(args[1]));
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}
