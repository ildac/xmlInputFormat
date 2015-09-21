package HDP.mapreduce_xml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.junit.Test;

public class XMLTest {
    
    @Test
    public void run() throws Exception {
        Path inputPath = new Path("/tmp/mrtest/input");
        Path outputPath = new Path("/tmp/mrtest/output");
        
        Configuration conf = new Configuration();
        
        conf.set("mapred.job.tracker", "local");
        conf.set("fs.defaultFS", "file:///");
        
        conf.set("key.value.separator.in.input.line", " ");
        conf.set(XMLRecordReader.START_TAG, "<property>");
        conf.set(XMLRecordReader.END_TAG, "</property>");
        
        
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }
        
        if (fs.exists(inputPath)) {
            fs.delete(inputPath, true);
        }
        
        fs.mkdirs(inputPath);
        
        String input = "<property><name>property Name</name><value>property value</value></property>";
        DataOutputStream file = fs.create(new Path(inputPath, "part-" +0));
        file.writeBytes(input);
        file.close();
        
        Job job = runJob(conf, inputPath, outputPath);
        assertTrue(job.isSuccessful());
        
        List<String> lines = IOUtils.readLines(fs.open(new Path(outputPath, "part-m-00000")));
        
        assertEquals(1,  lines.size());
        String[] parts = StringUtils.split(lines.get(0), "\t");
        assertEquals("property Name", parts[0]);
        assertEquals("property value", parts[1]);  
    }

    private Job runJob(Configuration conf, Path inputPath, Path outputPath) throws ClassNotFoundException, IOException, InterruptedException {
        Job job = Job.getInstance(conf);
        job.setJarByClass(XMLMapReduce.class);
        job.setInputFormatClass(XMLInputFormat.class);
        job.setMapperClass(Map.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setNumReduceTasks(0);

        job.setOutputFormatClass(TextOutputFormat.class);
        
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        job.waitForCompletion(false);
        return job;
    }
    
}
