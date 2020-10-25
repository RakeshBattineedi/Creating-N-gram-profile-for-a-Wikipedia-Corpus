import   java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import   java.util.StringTokenizer;
import   org.apache.hadoop.conf.Configuration;
import   org.apache.hadoop.fs.Path;
import   org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import   org.apache.hadoop.io.Text;
import   org.apache.hadoop.mapreduce.Job;
import   org.apache.hadoop.mapreduce.Mapper;
import   org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import   org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import   org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class profile3 {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	
        	if (!(value.toString().isEmpty())) {
        		StringTokenizer itr = new StringTokenizer(value.toString().split("<====>")[2]);
        		while (itr.hasMoreTokens()) {
        		String out = itr.nextToken().replaceAll("[^A-Za-z0-9]","").toLowerCase();
        		if(!out.isEmpty()) {
        		word.set(out);
        		context.write(word, one);
        		}
        		}
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable, Text, IntWritable> {
    	private HashMap<String, Integer> map = new HashMap<>(); 
        private int count=0;
        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
        	
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            map.put(key.toString(),sum);            
        }
        
        public void cleanup(Context context) throws IOException, InterruptedException {
        	
            List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(map.entrySet());
          
            list.sort(Entry.<String, Integer>comparingByValue().reversed());
         
            for(Map.Entry<String, Integer> entry:list){
            	if( count == 500 ) {
                    break;
                 }
      		context.write( new Text(entry.getKey()), new IntWritable(entry.getValue()));
      		count++;
      	}
            
      }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Profile 3");
        job.setJarByClass(profile3.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
