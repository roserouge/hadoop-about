import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestScripting {

	private static final Logger LOG = LoggerFactory
			.getLogger(TestScripting.class);

	public static void listEngines() {
		ScriptEngineManager mgr = new ScriptEngineManager();
		List<ScriptEngineFactory> factories = mgr.getEngineFactories();
		for (ScriptEngineFactory factory : factories) {
			LOG.info("ScriptEngineFactory Info");

			String engName = factory.getEngineName();
			String engVersion = factory.getEngineVersion();
			LOG.info("\tScript Engine: {} ({})", engName, engVersion);

			String langName = factory.getLanguageName();
			String langVersion = factory.getLanguageVersion();
			LOG.info("\tLanguage: {} ({})", langName, langVersion);

			List<String> engNames = factory.getNames();
			for (String name : engNames) {
				LOG.info("\tEngine Alias: {}", name);
			}
		}
	}

	public static void main(String[] args) {
		ScriptEngineManager mgr = new ScriptEngineManager();
		ScriptEngine engine = mgr.getEngineByName("groovy");

		Compilable compiler = (Compilable) engine;

		try {
			// engine.eval("println 'Hello, world!'");

			InputStream in0 = TestScripting.class
					.getResourceAsStream("/script.groovy");
			InputStreamReader reader0 = new InputStreamReader(in0);
			CompiledScript script = compiler.compile(reader0);
			script.eval();

			InputStream in = TestScripting.class
					.getResourceAsStream("/meta.groovy");
			InputStreamReader reader = new InputStreamReader(in);
			// InputStreamReader reader = new FileReader(
			// "d:\\work\\3.6\\i3.job\\script\\script.groovy");
			Object result = engine.eval(reader);
			LOG.info("{} {}", result.getClass().getName(), result.toString());
			List meta = (List) result;
			for (int i = 0; i < 3; i++) {
				List item = (List) meta.get(i);
				String name = (String) item.get(0);
				Class type = (Class) item.get(1);
				LOG.info("{} {} {}",
						new Object[] { name, type.getName(), item.get(2) });
			}
		} catch (ScriptException e) {
			LOG.error("", e);
		}

		// catch (FileNotFoundException e) {
		// LOG.error("", e);
		// }

		// listEngines();
	}
}
