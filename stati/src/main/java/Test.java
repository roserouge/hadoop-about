import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Test {

	private static final Logger LOG = LoggerFactory.getLogger(Test.class);

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		// HttpURLConnection conn = null;
		// try {
		// URL url = new URL("http://localhost/123");
		// conn = (HttpURLConnection) url.openConnection();
		//
		// InputStream in = conn.getInputStream();
		// print(in);
		// } catch (MalformedURLException e) {
		// LOG.error("", e);
		// } catch (IOException e) {
		// LOG.error("", e);
		//
		// print(conn.getErrorStream());
		// } finally {
		// if (conn != null) {
		// conn.disconnect();
		// }
		// }

		// String[] found = null;
		//
		// Pattern p = Pattern.compile("(.{3})(.{5})(.{4})");
		// Matcher m = p.matcher("abcdefghijkl");
		// if (m.matches()) {
		// int len = m.groupCount();
		// found = new String[len];
		// for (int i = 1; i <= len; i++) {
		// found[i - 1] = m.group(i);
		// }
		// }
		//
		// LOG.info(StringUtils.join(found, ", "));

		LOG.info("{}, {}", -8 >> 1, -8 >>> 1);
	}

	private static void print(InputStream input) throws IOException {
		BufferedInputStream in = new BufferedInputStream(input);
		byte[] buf = new byte[1024];
		int len = 0;
		while ((len = in.read(buf, 0, 1024)) > -1) {
			LOG.info("{}", new String(Arrays.copyOf(buf, len)));
		}
	}

}
