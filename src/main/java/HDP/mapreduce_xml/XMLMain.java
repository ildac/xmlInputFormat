package HDP.mapreduce_xml;

import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

public class XMLMain {

	public static void main(String[] args) throws Exception {
		System.out.println(new Date());
		int res = ToolRunner.run(new Configuration(), new XMLMapReduce(), args);
		System.out.println(new Date());
		System.exit(res);
	}
}