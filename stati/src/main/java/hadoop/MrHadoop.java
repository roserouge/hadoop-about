package hadoop;

import java.io.IOException;
import java.net.URI;

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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MrHadoop extends Configured implements Tool {

	private static final Logger LOG = LoggerFactory.getLogger(MrHadoop.class);

	private static final String TIMESTAMP = "timestamp";

	private static final String CACHE_FILE = "/user/hadoop/share/num.txt";

	public static class M extends
			Mapper<LongWritable, Text, LongWritable, Text> {

		// private String base = "/tmp";
		// private FileChannel channel = null;

		private Path theCacheFile;

		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// File file = new File(base);
			// file.mkdirs();
			//
			// channel = new FileInputStream(base + File.separator
			// + "sync_time.log").getChannel();

			Configuration conf = context.getConfiguration();

			// Path[] localFiles = DistributedCache.getLocalCacheFiles(conf);
			// theCacheFile = localFiles[0];

			theCacheFile = new Path("");
			long timestamp = FileSystem.get(conf).getFileStatus(theCacheFile)
					.getModificationTime();
			DistributedCache.getLocalCache(theCacheFile.toUri(), conf,
					new Path("."), false, timestamp, new Path("."));
		}

		@Override
		protected void cleanup(Context context) throws IOException,
				InterruptedException {
			// if (channel != null) {
			// channel.close();
			// }

			Configuration conf = context.getConfiguration();
			DistributedCache.releaseCache(theCacheFile.toUri(), conf);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			// ByteBuffer buffer = ByteBuffer.allocate(1024);
			// int size = 0;
			// while ((size = channel.read(buffer)) > -1) {
			// buffer.flip();
			// LOG.info(":p "
			// + new String(Arrays.copyOf(buffer.array(), size)));
			// buffer.clear();
			// }

			context.write(key, value);
		}

	}

	public int run(String[] args) throws Exception {
		String date = args[2];

		Configuration conf = getConf();
		getConf().set(TIMESTAMP, date);

		DistributedCache.addCacheFile(new URI(CACHE_FILE), conf);

		Job job = new Job(conf, date + "MrHadoop");
		job.setJarByClass(MrHadoop.class);

		// job.setMapperClass(M.class);

		TextInputFormat.addInputPaths(job, args[0]);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		boolean successful = job.waitForCompletion(true);

		System.out.println(job.getJobName()
				+ (successful ? " :successful" : " :failed"));

		return successful ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		System.out.println("MrHadoop Start...");
		System.exit(ToolRunner.run(new Configuration(), new MrHadoop(), args));
	}

}
