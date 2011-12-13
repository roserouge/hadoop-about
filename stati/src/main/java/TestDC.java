import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestDC extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(TestDC.class);

	public static class M extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		private BufferedReader reader = null;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			Configuration conf = context.getConfiguration();
			Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);

			reader = new BufferedReader(
					new FileReader(localFiles[0].toString()));
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			if (reader != null) {
				reader.close();
			}

		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String line = null;
			if ((line = reader.readLine()) != null) {
				LOG.info("<{}>line: {}", key.get(), line);
				System.out.println("<" + key.get() + "> line: " + line);
			}

			context.write(key, value);
		}

	}

	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

		Path local = new Path("/root/hi.cache");
		Path hdfs = new Path("/user/root/cache/hi");
		FileSystem fs = FileSystem.get(conf);
		fs.copyFromLocalFile(false, true, local, hdfs);
		DistributedCache.addCacheFile(hdfs.toUri(), conf);

		Job job = new Job(conf, "Test DistributedCache");
		job.setJarByClass(TestDC.class);

		job.setMapperClass(M.class);
		job.setNumReduceTasks(3);

		String[] files = args[0].split(",");
		for (String file : files) {
			TextInputFormat.addInputPath(job, new Path(file));
		}
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean successful = job.waitForCompletion(true);
		System.out.println(job.getJobName()
				+ (successful ? " :successful" : " :failed"));

		return successful ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("Hello World!");
		System.exit(ToolRunner.run(new Configuration(), new TestDC(), args));
	}

}
