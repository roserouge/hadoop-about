package mahout.recommender;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.IRStatistics;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.GenericRecommenderIRStatsEvaluator;
import org.apache.mahout.cf.taste.impl.eval.LoadEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.Recommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 * 
 */
public class App {

	private static final Logger LOG = LoggerFactory.getLogger(App.class);

	public static void main(String[] args) throws IOException, TasteException {
		LOG.info("hello");

		DataModel model = new FileDataModel(new File(
				"E:\\down\\ml-1m\\intro.csv"));

		// UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
		// UserNeighborhood neighborhood = new NearestNUserNeighborhood(3,
		// similarity, model);
		// Recommender recommender = new GenericUserBasedRecommender(model,
		// neighborhood, similarity);

		RecommenderBuilder builder = new RecommenderBuilder() {
			public Recommender buildRecommender(DataModel model)
					throws TasteException {
				UserSimilarity similarity = new PearsonCorrelationSimilarity(
						model);
				UserNeighborhood neighborhood = new NearestNUserNeighborhood(2,
						similarity, model);
				// new ThresholdUserNeighborhood(0.5, similarity, model);
				return new GenericUserBasedRecommender(model, neighborhood,
						similarity);
			}
		};

		// RecommenderBuilder builder = new RecommenderBuilder() {
		// public Recommender buildRecommender(DataModel model)
		// throws TasteException {
		// return new SlopeOneRecommender(model);
		// }
		// };

		List<RecommendedItem> recommendations = builder.buildRecommender(model)
				.recommend(1, 3);
		for (RecommendedItem recommendation : recommendations) {
			LOG.info("{}", recommendation);
		}

		RandomUtils.useTestSeed();

		// RecommenderEvaluator evaluator = new
		// AverageAbsoluteDifferenceRecommenderEvaluator();
		// double score = evaluator.evaluate(builder, null, model, 0.7, 1.0);
		// LOG.info("{}", score);

		RecommenderIRStatsEvaluator irEvaluator = new GenericRecommenderIRStatsEvaluator();
		IRStatistics stats = irEvaluator.evaluate(builder, null, model, null,
				2, GenericRecommenderIRStatsEvaluator.CHOOSE_THRESHOLD, 1.0);
		LOG.info("{}", stats.getPrecision());
		LOG.info("{}", stats.getRecall());

		LoadEvaluator.runLoad(builder.buildRecommender(model));
	}

}
