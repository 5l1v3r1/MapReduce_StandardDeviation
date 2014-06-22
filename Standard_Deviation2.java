import java.io.*;
import java.util.*;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class GoogleBooksProb2b extends Configured implements Tool { 

    public static class Mapper1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

    private static OutputCollector<IntWritable, DoubleWritable> collector = null;	
    DoubleWritable sumOfSquares = new DoubleWritable();
	IntWritable key = new IntWritable(1);  
	double sumOfSquaresTemp;
	double avg;

	public void configure(JobConf job) {
		avg = 13.426;
		sumOfSquaresTemp = 0.0;
	}

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
		    String[] tokens = value.toString().split("\\t");
			    		sumOfSquaresTemp =  sumOfSquaresTemp + (Math.pow(((Integer.parseInt(tokens[3].trim())) - avg),2));
			    		collector = output;
					}
			    
		public void close() throws IOException{
			sumOfSquares.set(sumOfSquaresTemp);
			collector.collect(key, sumOfSquares);
		}
	}

		
	public static class Mapper2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

    private OutputCollector<IntWritable, DoubleWritable> collector = null;
    DoubleWritable sumOfSquares = new DoubleWritable();
	IntWritable key = new IntWritable(1); 
	double sumOfSquaresTemp;
	double avg;

	public void configure(JobConf job) {
		avg = 13.426;
		sumOfSquaresTemp = 0.0;
	}

	public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
	    String[] tokens = value.toString().split("\\s+|\\t+");
		    		sumOfSquaresTemp =  sumOfSquaresTemp + (Math.pow(((Integer.parseInt(tokens[4].trim())) - avg),2));
		    		collector = output;
				}
			    
	public void close() throws IOException{
		sumOfSquares.set(sumOfSquaresTemp);
		collector.collect(key, sumOfSquares);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

		public void reduce(IntWritable key, Iterator<DoubleWritable> values, OutputCollector<IntWritable, DoubleWritable> collector, Reporter reporter) throws IOException {
	   	double sum = 0;
	    double sd = 0;
	    int countTemp = 129655807;
	    while (values.hasNext()) {
				sum = sum + values.next().get();
	    	}
	    sd = Math.sqrt((sum/countTemp));
	    collector.collect(key, new DoubleWritable(sd));
	}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), GoogleBooksProb2b.class);
	//conf.setNumReduceTasks(0);
	conf.setJobName("GoogleBooksProb2b");

	conf.setOutputKeyClass(IntWritable.class);
	conf.setOutputValueClass(DoubleWritable.class);

	//conf.setMapperClass(Map.class);
	//conf.setCombinerClass(Reduce.class);
	conf.setReducerClass(Reduce.class);

	conf.setInputFormat(TextInputFormat.class);
	conf.setOutputFormat(TextOutputFormat.class);

	//FileInputFormat.setInputPaths(conf, new Path(args[0]));
	//FileInputFormat.setInputPaths(conf, new Path(args[1]));
	
	MultipleInputs.addInputPath(conf, new Path(args[0]), TextInputFormat.class, Mapper1.class);
	MultipleInputs.addInputPath(conf, new Path(args[1]), TextInputFormat.class, Mapper2.class);
	FileOutputFormat.setOutputPath(conf, new Path(args[2]));

	JobClient.runJob(conf);
	return 0;
    }

    public static void main(String[] args) throws Exception {
	int res = ToolRunner.run(new Configuration(), new GoogleBooksProb2b(), args);
	System.exit(res);
    }
}



