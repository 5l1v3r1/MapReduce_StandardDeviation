import java.io.*;
import java.util.*;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.lib.MultipleInputs;

public class GoogleBooksProb2 extends Configured implements Tool { 

    public static class Mapper1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

    private OutputCollector<IntWritable, DoubleWritable> collector = null;
    DoubleWritable volumes = new DoubleWritable();
	IntWritable count = new IntWritable(); 
	int countTemp;
	int volumesTemp;

	public void configure(JobConf job) {
		countTemp = 0;
		volumesTemp = 0;
	}

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
		    String[] tokens = value.toString().split("\\t");
			    		countTemp++;
			    		volumesTemp =  volumesTemp + Integer.parseInt(tokens[3].trim());
			    		collector = output;
					}
			    
		public void close() throws IOException{
			count.set(countTemp);
			volumes.set(volumesTemp);
			collector.collect(count, volumes);
		}
	}

		
	public static class Mapper2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, DoubleWritable> {

		private OutputCollector<IntWritable, DoubleWritable> collector = null;
		DoubleWritable volumes = new DoubleWritable();
		IntWritable count = new IntWritable(); 
		int countTemp;
		int volumesTemp; 

		public void configure(JobConf job) {
			countTemp = 0;
			volumesTemp = 0;
		}

		public void map(LongWritable key, Text value, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
		    String[] tokens = value.toString().split("\\s+|\\t+");
			    		countTemp++;
			    		volumesTemp =  volumesTemp + Integer.parseInt(tokens[4].trim());
			    		collector = output;
					}
			    
		public void close() throws IOException{
			count.set(countTemp);
			volumes.set(volumesTemp);
			collector.collect(count, volumes);
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
		
	private OutputCollector<IntWritable, DoubleWritable> collector = null;	
	DoubleWritable values = new DoubleWritable();
	IntWritable key = new IntWritable(); 
	double sumSum;
	int sumKey;

		public void configure(JobConf job) {
			sumSum = 0;
			sumKey = 0;
		}

		public void reduce(IntWritable key, Iterator<DoubleWritable> values, OutputCollector<IntWritable, DoubleWritable> output, Reporter reporter) throws IOException {
	     	while (values.hasNext()) {
	 			sumSum = sumSum + values.next().get();
	     	}
	     	sumKey = sumKey + Integer.parseInt(key.toString());
	 		collector = output;
	 	}

	 	public void close() throws IOException{
	 		double mean = (sumSum/sumKey);
	 		key.set(sumKey);
			values.set(mean);
			collector.collect(key, values);
		}
    }

    public int run(String[] args) throws Exception {
	JobConf conf = new JobConf(getConf(), GoogleBooksProb2.class);
	conf.setNumReduceTasks(1);
	conf.setJobName("GoogleBooksProb2");

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
	int res = ToolRunner.run(new Configuration(), new GoogleBooksProb2(), args);
	System.exit(res);
    }
}



