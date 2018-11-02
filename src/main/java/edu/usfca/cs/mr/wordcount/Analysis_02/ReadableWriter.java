package edu.usfca.cs.mr.wordcount.Analysis_02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;


public class ReadableWriter implements Writable {
    private LongWritable sentenceCount;
    private LongWritable wordsCount;
    private LongWritable syllabifyCount;
    private LongWritable complexCount;
    private static SentenceExtractor se = new SentenceExtractor();


    private Text comment;

    public ReadableWriter() {
        this.sentenceCount = new LongWritable(0);
        this.wordsCount = new LongWritable(0);
        this.syllabifyCount = new LongWritable(0);
        this.complexCount = new LongWritable(0);
    }

    public ReadableWriter(String comment) {
        this();
        this.comment = new Text(comment);
    }


    public LongWritable getSentenceCount() {
        return sentenceCount;
    }

    public LongWritable getWordsCount() {
        return wordsCount;
    }

    public LongWritable getSyllabifyCount() {
        return syllabifyCount;
    }

    public LongWritable getComplexCount() {
        return complexCount;
    }


    public void setSentenceCount(String body) {
        this.sentenceCount =  new LongWritable(this.getNumberOfSentences(body));
    }

    public void setWordsCount(String body) {
        this.wordsCount = new LongWritable(this.getNumberOfWords(body));
    }

    public void setSyllabifyCount(String body) {
        this.syllabifyCount = new LongWritable(this.getNumberOfSyllables(body));
    }

    public void setcomplexCount(String body) {
        this.complexCount =  new LongWritable(this.getNumberOfComplexWords(body));
    }


    public void increment(ReadableWriter rw) {
        incrementSentenceCount(rw.sentenceCount.get());
        incrementWordsCount(rw.wordsCount.get());
        incrementSyllabifyCount(rw.syllabifyCount.get());
        incrementComplexCount(rw.complexCount.get());
    }

    private void incrementSentenceCount(long count) {
        this.sentenceCount.set(this.sentenceCount.get() + count);
    }

    public void incrementWordsCount(long count) {
        this.wordsCount.set(this.wordsCount.get() + count);
    }

    private void incrementSyllabifyCount(long count) {
        this.syllabifyCount.set(this.syllabifyCount.get() + count);
    }

    public void incrementComplexCount(long count) {
        this.complexCount.set(this.complexCount.get() + count);
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


    private int getNumberOfComplexWords(String text) {
        String cleanText = cleanLine(text);
        String[] words = cleanText.split(" ");
        int complex = 0;
        for (String w : words) {
            if (isComplex(w))
                complex++;
        }
        return complex;
    }

    private boolean isComplex(String w) {
        int syllables = Syllabify.syllable(w);
        return (syllables > 2);
    }

    private int getNumberOfSyllables(String text) {
        String cleanText = cleanLine(text);
        String[] word = cleanText.split(" ");
        int syllables = 0;
        for (String w : word) {
            if (w.length() > 0) {
                syllables += Syllabify.syllable(w);
            }
        }
        return syllables;
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

    private int getNumberOfSentences(String text) {
        int l = se.getSentences(text).length;
        if (l > 0)
            return l;
        else if (text.length() > 0)
            return 1;
        else
            return 0;
    }

    public double getFleschKincaidGradeLevel() {
        double score = 0.39 * wordsCount.get() / sentenceCount.get() + 11.8 * sentenceCount.get() / wordsCount.get()
                - 15.59;
//        return round(score, 3);
        return score;
    }

    public double getFleschReadingEase() {

        double score = 206.835 - 1.015 * wordsCount.get() / sentenceCount.get() - 84.6 * syllabifyCount.get() / wordsCount.get();
//        return round(score, 3);
        return score;
    }

    public double getGunningFog() {
        double score = 0.4 * (wordsCount.get() / sentenceCount.get() + 100 * complexCount.get() / wordsCount.get());
//        return round(score, 3);
        return score;

    }

    private double round(double d, int decimalPlace) {
        // see the Javadoc about why we use a String in the constructor
        // http://java.sun.com/j2se/1.5.0/docs/api/java/math/BigDecimal.html#BigDecimal(double)
        BigDecimal bd = new BigDecimal(Double.toString(d));
        bd = bd.setScale(decimalPlace, BigDecimal.ROUND_HALF_UP);
        return bd.doubleValue();
    }

    public boolean hasCount() {
        return this.wordsCount.get() > 0
                && this.sentenceCount.get() > 0;
    }

    public void readFields(DataInput in) throws IOException {

        this.sentenceCount.readFields(in);
        this.wordsCount.readFields(in);
        this.syllabifyCount.readFields(in);
        this.complexCount.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
        this.sentenceCount.write(out);
        this.wordsCount.write(out);
        this.syllabifyCount.write(out);
        this.complexCount.write(out);
    }


    @Override
    public String toString() {
        return "sentenceCount:" + sentenceCount.toString()
                + "\t" + "wordsCount: " + wordsCount.toString()
                + "\t" + "syllabifyCount: " + syllabifyCount.toString()
                + "\t" + "complexCount: " + complexCount.toString()
                + "\n" + "GunningFog: " + this.getGunningFog()
                + "\n" + "FleschKincaidGradeLevel: " + this.getFleschKincaidGradeLevel()
                + "\n" + "FleschReadingEase: " + this.getFleschReadingEase();
    }

}
