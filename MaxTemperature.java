/*
  To learn different ways of execution of MR like default mapper , default reducer and so on ..
  This helps understanding different way the data gets changed during the execution of MR. 
  
  Input data:

0067011990999991950051507004...9999999N9+00001+99999999999...
0043011990999991950051512004...9999999N9+00221+99999999999...
0043011990999991950051518004...9999999N9-00111+99999999999...
0043012650999991949032412004...0500001N9+01111+99999999999...
0043012650999991949032418004...0500001N9+00781+99999999999...

  Output by changing the config class : 
  
  Run1 : 
  Code: [  This is the given Map and Reducer and Output Key -Value pair as general ]
  -------
       job.setOutputKeyClass(Text.class);
	     job.setOutputValueClass(IntWritable.class);

	 	   job.setMapperClass(MTmapper.class);
	     job.setReducerClass(MTreducer.class);
  -------
  Output 
  ------- 
  1949	111
  1950	22
  
  Run 2:
   Code: [  Without any Map or Reducer. Obser the output is Longwriteable & Text ]
  -------
     //  job.setOutputKeyClass(Text.class);
	   //  job.setOutputValueClass(IntWritable.class);

	   //  job.setMapperClass(MTmapper.class);
	  // job.setReducerClass(MTreducer.class);
  -------
  Output 
  -------  
0	0067011990999991950051507004...9999999N9+00001+99999999999...
62	0043011990999991950051512004...9999999N9+00221+99999999999...
124	0043011990999991950051518004...9999999N9-00111+99999999999...
186	0043012650999991949032412004...0500001N9+01111+99999999999...
248	0043012650999991949032418004...0500001N9+00781+99999999999... 

   Run 3:
   Code: [ with out setting the output key& value pairs ]
  -------
     //  job.setOutputKeyClass(Text.class);
	   //  job.setOutputValueClass(IntWritable.class);

	     job.setMapperClass(MTmapper.class);
	   job.setReducerClass(MTreducer.class);
  -------
  Output 
  -------  
      java.lang.Exception: java.io.IOException: Type mismatch in key from map: expected org.apache.hadoop.io.LongWritable, 
      received org.apache.hadoop.io.Text
	  
  Run 4:
   Code: [ No reducer ]
  -------
     job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(IntWritable.class);

	   job.setMapperClass(MTmapper.class);
	  // job.setReducerClass(MTreducer.class);
  -------
  Output 
  -------  
1949	78
1949	111
1950	11
1950	22
1950	0

	  
  Run 5:
   Code: [ neither mapper nor reducer ] 
  -------
     job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(IntWritable.class);

	 //  job.setMapperClass(MTmapper.class);
	 //  job.setReducerClass(MTreducer.class);
  -------
  Output 
  -------  
      java.lang.Exception: java.io.IOException: Type mismatch in key from map: expected org.apache.hadoop.io.Text,
      received org.apache.hadoop.io.LongWritable
 
  Run 6:
   Code: [ No mapper ]
  -------
     job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(IntWritable.class);

	     //  job.setMapperClass(MTmapper.class);
	     job.setReducerClass(MTreducer.class);
  -------
  Output 
  -------  
    java.lang.Exception: java.io.IOException: Type mismatch in key from map: expected org.apache.hadoop.io.Text, 
    received org.apache.hadoop.io.LongWritable
--------------------------
   Run7:
   Code: [ Add more reducers with out actually taking care in code ]
   ------
    //Added 
    job.setNumReduceTasks(2); // may or may not give the equal distribution....

Output:
$ ls -lrt
total 4
-rw-r--r-- 1 cloudera cloudera 17 Oct 27 11:54 part-r-00000
-rw-r--r-- 1 cloudera cloudera  0 Oct 27 11:55 part-r-00001
-rw-r--r-- 1 cloudera cloudera  0 Oct 27 11:55 _SUCCESS
$ cat part-r-00000
1949	111
1950	22
$ cat part-r-00001
$ 



*/

import java.io.IOException;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MaxTemperature {
	
	public static class MTmapper extends Mapper<LongWritable,Text,Text,IntWritable> {

		public Text year=new Text("0000");
		public IntWritable airtemp=new IntWritable(0);
	  	 
		public void map(LongWritable key, Text value, Context context)
			      throws IOException, InterruptedException {
			    
			    String line = value.toString();
			   // FileSplit fileSplit = (FileSplit)context.getInputSplit();
			   // String fileName = fileSplit.getPath().getName();
			    year.set( line.substring(15, 19));
			  //  System.out.println("VSSR : Filen name :"+fileName);
			    
			    //int airTemperature;
			    if (line.charAt(41) == '+') { // parseInt doesn't like leading plus signs
			      airtemp.set(Integer.parseInt(line.substring(41, 45)));
			    } else {
			      airtemp.set(Integer.parseInt(line.substring(41, 45)));
			    }
			    
			    System.out.println("VSSR:"+year+" "+airtemp);
			    			      context.write(year, airtemp);
			   
			  }

		
	}
    public static class MTreducer extends Reducer<Text,IntWritable, Text,IntWritable> {
       
        public void reduce (Text key,Iterable<IntWritable> values,Context context)
               throws IOException , InterruptedException {

            int maxOfMonth= Integer.MIN_VALUE;
            
             for (IntWritable value : values  ) 
             {
            	 maxOfMonth = Math.max(maxOfMonth, value.get());
             }
            
             context.write(key, new IntWritable(maxOfMonth));

        }
		
	}
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		
		   Job job= new Job();
		   job.setJarByClass(MaxTemperature.class);
		   job.setJobName("Max Temperature");
		
		 
		   job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(IntWritable.class);
          
		   job.setMapperClass(MTmapper.class);
		   job.setReducerClass(MTreducer.class);
		   //job.setNumReduceTasks(2);
		
		   FileInputFormat.addInputPath(job, new Path(args[0]));
		   FileOutputFormat.setOutputPath(job, new Path(args[1]));

		   System.exit( job.waitForCompletion(true) ? 0:1 );
		   
	}
	
	
}
