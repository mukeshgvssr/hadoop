import org.apache.hadoop.conf.Configured;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertIndex extends Configured  {
	public static void main(String[] args) throws Exception {
		if (args.length != 2) {
			System.out.println("Usage: [input] [output]");
			System.exit(-1);
		}

		Job job = new Job();
		job.setJobName("InvertIndex");
		job.setJarByClass(InvertIndex.class);
		
		/* Field separator for reducer output*/
		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " | ");
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(InvertIndexMapper.class);
		job.setCombinerClass(InvertIndexReducer.class);
		job.setReducerClass(InvertIndexReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path inputFilePath = new Path(args[0]);
		Path outputFilePath = new Path(args[1]);

		/* This line is to accept input recursively */
		FileInputFormat.setInputDirRecursive(job, true);

		FileInputFormat.addInputPath(job, inputFilePath);
		FileOutputFormat.setOutputPath(job, outputFilePath);


		System.exit( job.waitForCompletion(true) ? 0 : 1);
	}

	
		
	

public static class InvertIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    private Text word = new Text();
    private Text filename = new Text();

	private boolean caseSensitive = false;


	public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
		String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
		filename = new Text(filenameStr);

		String line = value.toString();

		if (!caseSensitive) {
			line = line.toLowerCase();
		}

		StringTokenizer tokenizer = new StringTokenizer(line);
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());			
			context.write(word, filename);
		}
	}

 }
	public static class InvertIndexReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(final Text key, final Iterable<Text> values,
		final Context context) throws IOException, InterruptedException {

	StringBuilder stringBuilder = new StringBuilder();

	for (Text value : values) {
		stringBuilder.append(value.toString());

		if (values.iterator().hasNext()) {
			stringBuilder.append(" -> ");
		}
	}

	context.write(key, new Text(stringBuilder.toString()));
	}

	}
}
/**************
Input:
$ cat doc1.txt
mukesh one two three four
$ cat doc2.txt
suma  five six seven eight

Output: 

$ cat part-r-00000 
eight | doc2.txt
five | doc2.txt
four | doc1.txt
mukesh | doc1.txt
one | doc1.txt
seven | doc2.txt
six | doc2.txt
suma | doc2.txt
three | doc1.txt
two | doc1.txt


**************/

