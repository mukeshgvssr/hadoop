/*
   General Word count program. 
   Example for usage of Partitioner logic
*/

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import org.apache.hadoop.mapred.Partitioner;
import java.util.Arrays;
 
public class MWPCount {

	/**
	 * @param args
	 */
	
	public static class MWPmapper extends MapReduceBase 
	implements Mapper<LongWritable,Text,Text,IntWritable> {
	    
	    private Text word = new Text();
	 
	    public void map (LongWritable key, Text value,
	           OutputCollector<Text, IntWritable> output,Reporter reporter)
	           throws IOException {

	        String line = value.toString();
	        StringTokenizer tokenizer = new StringTokenizer(line);
	        while (tokenizer.hasMoreTokens()) 
	        {   
	             word.set(tokenizer.nextToken());
	             output.collect(word, new IntWritable(1));
	         }

	    } 
		
	}
    public static class MWPreducer extends MapReduceBase 
       implements Reducer<Text,IntWritable, Text,IntWritable> {
       
        public void reduce (Text key,Iterator<IntWritable> values,
               OutputCollector<Text, IntWritable> output,Reporter reporter)
               throws IOException {

            int total=0;
            
            while (values.hasNext()) 
            {   
                 total+=values.next().get();
                 
             }
            output.collect(key, new IntWritable(total));

        }
		
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		JobConf conf = new JobConf(MWPCount.class);
		   conf.setJobName("My Word Count");
		
		   conf.setOutputKeyClass(Text.class);
		   conf.setOutputValueClass(IntWritable.class);

		   conf.setMapperClass(MWPmapper.class);
		   conf.setReducerClass(MWPreducer.class);
		   conf.setPartitionerClass(MWPPartioner.class);
		   conf.setNumReduceTasks(3);

		   conf.setInputFormat(TextInputFormat.class);
		   conf.setOutputFormat(TextOutputFormat.class);

		   FileInputFormat.addInputPath(conf, new Path(args[0]));
		   FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		   JobClient.runJob(conf);
		   
	}
	
	public static class MWPPartioner implements Partitioner<Text, IntWritable>  {

	       public int getPartition( Text key, IntWritable value, int numReduceTasks) {

	          String[] mywords = { "hadoop","mukesh" };
	          String[] numwords = {"one","two","three"};

	          Integer reducerNumber =0;

	          if( Arrays.asList(mywords).contains(key.toString())) {
	                 reducerNumber = 1;

	          } else if( Arrays.asList(numwords).contains(key.toString())) {
	                 reducerNumber = 2;

	          } 
	     
	           return reducerNumber;


	       }

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			
		}

	  }

}
