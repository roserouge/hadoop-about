import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Comparator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestMapFile {

	private static final Logger LOG = LoggerFactory
			.getLogger(TestMapFile.class);

	// static {
	// PropertyConfigurator.configure(MapFileTest.class
	// .getResource("/log4j.properties"));
	// }

	public static void main(String[] args) throws IOException,
			URISyntaxException {
		String[][] data = { { "zero", "0" }, { "one", "1" }, { "two", "2" },
				{ "three", "3" }, { "four", "4" }, { "five", "5" },
				{ "six", "6" }, { "seven", "7" }, { "eight", "8" },
				{ "nine", "9" } };
		Arrays.sort(data, new Comparator<String[]>() {
			public int compare(String[] a1, String[] a2) {
				return a1[0].compareTo(a2[0]);
			}
		});

		Configuration conf = new Configuration();
		String dirName = "map";

		// dirName = cache(conf);
		// write(conf, dirName, data);
		read(conf, dirName);

		LOG.info("{}", conf.get("mapred.cache.files"));
		LOG.info("{}", conf.get("mapred.cache.localFiles"));
	}

	public static String cache(Configuration conf) throws IOException,
			URISyntaxException {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(conf);

			Path src = new Path("map");
			// Path dest = new Path("cache");

			// fs.copyFromLocalFile(false, true, src, dest);
			DistributedCache.addCacheFile(src.toUri(), conf);

			Path cache = DistributedCache.getLocalCache(src.toUri(), conf,
					new Path("."), false, 0, new Path("."));
			String dirName = cache.toUri().getPath();
			LOG.info("cache to {}", dirName);

			DistributedCache.releaseCache(cache.toUri(), conf);
			return cache.toUri().getPath();

			// Path[] cache = DistributedCache.getLocalCacheFiles(conf);
			// LOG.info("{}", cache.length);
		} finally {
			IOUtils.closeStream(fs);
		}
	}

	public static void write(Configuration conf, String dirName,
			String[][] pairs) throws IOException, URISyntaxException {
		LOG.info("write to {}", dirName);

		FileSystem fs = null;
		MapFile.Writer writer = null;
		try {
			fs = FileSystem.get(conf);
			writer = new MapFile.Writer(conf, fs, dirName, Text.class,
					Text.class);
			for (String[] pair : pairs) {
				writer.append(new Text(pair[0]), new Text(pair[1]));
			}
		} finally {
			IOUtils.closeStream(writer);
			IOUtils.closeStream(fs);
		}
	}

	public static void read(Configuration conf, String dirName)
			throws IOException, URISyntaxException {
		LOG.info("read from {}", dirName);

		FileSystem fs = null;
		MapFile.Reader reader = null;
		try {
			fs = FileSystem.get(conf);
			reader = new MapFile.Reader(fs, dirName, conf);

			Text k = new Text();
			Text v = new Text();
			while (reader.next(k, v)) {
				LOG.info("{}: {}", k.toString(), v.toString());
			}

			String key = "two";
			Text value = (Text) reader.get(new Text(key), v);
			LOG.info("{} => {}, {}",
					new String[] { key, v.toString(), value.toString() });
		} finally {
			IOUtils.closeStream(reader);
			IOUtils.closeStream(fs);
		}
	}

}
