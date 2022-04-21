package samzatask.twitter.model.classifier;

import samzatask.config.Configuration;

/**
 *
 * @author mayconbordin
 */
public interface SentimentClassifier {
    public void initialize(Configuration config);
    public SentimentResult classify(String str);
}
