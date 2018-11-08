package edu.usfca.cs.mr.wordcount.Analysis_04;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;


public class SentimentAnalysisWritable implements Writable {

    private LongWritable positiveCount;
    private LongWritable negativeCount;

    private String text;
    private Map<String, Integer> positive;
    private Map<String, Integer> negative;
    private long words;

    public SentimentAnalysisWritable() {
        positiveCount = new LongWritable(0);
        negativeCount = new LongWritable(0);

    }

    public SentimentAnalysisWritable(String text, Map<String, Integer> positive, Map<String, Integer> negative) {
        positiveCount = new LongWritable(0);
        negativeCount = new LongWritable(0);
        this.text = text;
        this.positive = positive;
        this.negative = negative;
        this.words = getNumberOfWords(text);
        this.setimentAnalysisCheck();
    }

    public LongWritable getNegativeCount() {
        return negativeCount;
    }

    public LongWritable getPositiveCount() {
        return positiveCount;
    }

    public void setimentAnalysisCheck() {
        String cleanText = cleanLine(text);
        String[] word = cleanText.split(" ");
        for (String w : word) {
            if (positive.containsKey(w))
                incrementPositiveWords();
            else if (negative.containsKey(w))
                incrementNegativeWords();
        }
    }

    public void increment(SentimentAnalysisWritable sw) {
        incrementPositiveWords(sw.positiveCount.get());
        incrementNegativeWords(sw.negativeCount.get());
    }

    public boolean hasCount() {
        return this.negativeCount.get() > 0
                || this.positiveCount.get() > 0;
    }

    private void incrementPositiveWords(long count) {
        this.positiveCount.set(this.positiveCount.get() + count);
    }

    private void incrementNegativeWords(long count) {
        this.negativeCount.set(this.negativeCount.get() + count);
    }


    public void incrementPositiveWords() {
        this.incrementPositiveWords(1);
    }

    public void incrementNegativeWords() {
        this.incrementNegativeWords(1);
    }


    private int getNumberOfWords(String text) {
        String cleanText = cleanLine(text);
        String[] word = cleanText.split(" ");
        int words = 0;
        for (String w : word) {
            if (w.length() > 0)
                words++;
        }
        return words;
    }


    private String cleanLine(String line) {
        StringBuffer buffer = new StringBuffer();
        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);
            if (c < 128 && Character.isLetter(c)) {
                buffer.append(c);
            } else {
                buffer.append(' ');
            }
        }
        return buffer.toString().toLowerCase();
    }


    private double round(double d, int decimalPlace) {
        // see the Javadoc about why we use a String in the constructor
        // http://java.sun.com/j2se/1.5.0/docs/api/java/math/BigDecimal.html#BigDecimal(double)
        BigDecimal bd = new BigDecimal(Double.toString(d));
        bd = bd.setScale(decimalPlace, BigDecimal.ROUND_HALF_UP);
        return bd.doubleValue();
    }


    public void readFields(DataInput in) throws IOException {
        this.positiveCount.readFields(in);
        this.negativeCount.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        this.positiveCount.write(out);
        this.negativeCount.write(out);
    }


    @Override
    public String toString() {
        return "positiveCount: " + positiveCount.toString()
                + "\t" + "negativeCount: " + negativeCount.toString();
    }

}
