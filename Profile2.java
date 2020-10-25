import   java.io.IOException;
import   java.util.ArrayList;
import   java.util.List;
import   java.util.StringTokenizer;
import   org.apache.hadoop.conf.Configuration;
import   org.apache.hadoop.fs.Path;
import   org.apache.hadoop.io.IntWritable;
import   org.apache.hadoop.io.LongWritable;
import   org.apache.hadoop.io.NullWritable;
import   org.apache.hadoop.io.Text;
import   org.apache.hadoop.io.WritableComparable;
import   org.apache.hadoop.io.WritableComparator;
import   org.apache.hadoop.mapreduce.Job;
import   org.apache.hadoop.mapreduce.Mapper;
import   org.apache.hadoop.mapreduce.Reducer;
import   org.apache.hadoop.mapreduce.Reducer.Context;
import   org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import   org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class Profile2 {

    public static class Mapper1 extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
       
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        	if (!(value.toString().isEmpty())) {
        		StringTokenizer itrID = new StringTokenizer(value.toString().split("<====>")[1]);
        		String DocID = itrID.nextToken().toString();
        		StringTokenizer itr = new StringTokenizer(value.toString().split("<====>")[2]);
        		while (itr.hasMoreTokens()) {
        		String out = itr.nextToken().replaceAll("[^A-Za-z0-9]","").toLowerCase();
        		if(!out.isEmpty()) {
        		String CompKey = DocID+"\t"+out;
        		context.write(new Text(CompKey.toString()), one);
        		}
                
            }
        }
    }
 }
    
    public static class Reducer1 extends Reducer<Text,IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();
       
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        	  int sum = 0;
              for (IntWritable val : values) {
                  sum += val.get();
              }
              result.set(sum);
              context.write(key, result);
        }
    }
    
    public static class Mapper2 extends Mapper<LongWritable, Text, Text, NullWritable>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			
			String CompKey = value.toString();
		        context.write(new Text(CompKey.toString()), NullWritable.get());
		    }
		}

	 public static class Reducer2 extends Reducer<Text,NullWritable, Text, NullWritable> {
		 private  List<String> list = new ArrayList<String>();
		 private int noOfUnigramsSeenSoFar=0;
		 private String DocumentID="";
	        public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {	        	
	        	String[] t1 = key.toString().split("\t");
	        	String DocIDFromInputKey = t1[0];
	        	if(DocumentID.isEmpty()) {
	        		 DocumentID = DocIDFromInputKey;
	        	}
	        	if(DocumentID.equals(DocIDFromInputKey)) {
	        		if(noOfUnigramsSeenSoFar<=500) {
	        		list.add(key.toString()); 
	        		noOfUnigramsSeenSoFar++;
	        		}
	        	}
	        	else {
	        		DocumentID = DocIDFromInputKey;
	        		noOfUnigramsSeenSoFar=1;
	        		list.add(key.toString()); 
	        	}	            
	        
	        }
	        public void cleanup(Context context) throws IOException, InterruptedException {
	        	for (String out : list) {
	        		context.write( new Text(out), NullWritable.get());
	        	}
	        }
	 }    
	 
	 public static class myGroupComp extends WritableComparator {
		 protected myGroupComp() {
		 super(Text.class, true);
		 }
		 @Override
		 public int compare(WritableComparable w1, WritableComparable w2) {
		 Text t1 = (Text) w1;
		 Text t2 = (Text) w2;
		 String[] t1Items = t1.toString().split("\t");
		 String[] t2Items = t2.toString().split("\t");
		 Integer docId1=Integer.parseInt(t1Items[0]);
		 Integer docId2=Integer.parseInt(t2Items[0]);
		 String unigram1 = t1Items[1];
		 String unigram2 = t2Items[1];
		 Integer frequency1=Integer.parseInt(t1Items[2]);
		 Integer frequency2=Integer.parseInt(t2Items[2]);
		 Integer comparison = docId1.compareTo(docId2);
		 if (comparison == 0){
		 comparison = -1 * frequency1.compareTo(frequency2);
		 if (comparison == 0){
		 comparison = unigram1.compareTo(unigram2);
		 }
		 }
		 return comparison;
		 }
		 }
    


    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Job1");
        job1.setJarByClass(Profile2.class);
        job1.setMapperClass(Mapper1.class);
        job1.setCombinerClass(Reducer1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Job2");
        job2.setJarByClass(Profile2.class);
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setSortComparatorClass(myGroupComp.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(NullWritable.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
