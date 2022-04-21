package samzaapps.data;

import org.codehaus.jackson.annotate.JsonProperty;

public class WordCount {
	private final String word;
  	private final int count;
  	/**
   * Constructs a word count.
   *
   * @param word 
   * @param count
   */
  public WordCount(
      @JsonProperty("word") String word,
      @JsonProperty("count") int count) {
    this.word = word;
    this.count = count;
  }

  public String getWord() {
    return word;
  }

  public int getCount() {
    return count;
  }
}