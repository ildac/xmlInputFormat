package HDP.mapreduce_xml;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class XMLMapReduce extends Configured implements Tool {
	
	@SuppressWarnings("unused")
	private static final Logger log = LoggerFactory.getLogger(XMLMapReduce.class);
	
	public int run(String[] args) throws Exception {

        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
	    
	    Configuration conf = super.getConf();

	    conf.set("key.value.separator.in.input.line", " ");
	    conf.set(XMLRecordReader.START_TAG, "<property>");
	    conf.set(XMLRecordReader.END_TAG, "</property>");

	    Job job = Job.getInstance(conf);
	    job.setJarByClass(XMLMapReduce.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    job.setMapperClass(Map.class);
	    job.setInputFormatClass(XMLInputFormat.class);
	    job.setNumReduceTasks(0);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    FileInputFormat.setInputPaths(job, inputPath);
	    FileOutputFormat.setOutputPath(job, outputPath);

	    if (job.waitForCompletion(true)) {
	      return 0;
	    }
	    return 1;
	  }

}

