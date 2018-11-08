package edu.usfca.cs.mr.wordcount.Analysis_08;

import org.apache.hadoop.io.*;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import static edu.usfca.cs.mr.wordcount.Analysis_08.WordCountMapper.*;

public class MusicWritable implements Writable {
    private LongWritable blue;
    private LongWritable classical;
    private LongWritable electronica;
    private LongWritable folk;
    private LongWritable gospel;
    private LongWritable hip;
    private LongWritable jazz;
    private LongWritable metal;


    public MusicWritable(){
    }

    public MusicWritable(String text){
        this.blue = new LongWritable(this.getNumberOfWords(text));
        this.findRegexInBody(text);
    }



    private void incrementBluesCount(long count) {
        this.blue.set(this.blue.get() + count);
    }
    public void incrementBluesCount() {
        this.incrementBluesCount(1);
    }

    private void incrementClassicalCount(long count) {
        this.classical.set(this.classical.get() + count);
    }
    public void incrementClassicalCount() {
        this.incrementClassicalCount(1);
    }

    private void incrementElectronicaCount(long count) {
        this.electronica.set(this.electronica.get() + count);
    }
    public void incrementElectronicaCount() {
        this.incrementElectronicaCount(1);
    }

    private void incrementFolkCount(long count) {
        this.folk.set(this.folk.get() + count);
    }
    public void incrementFolkCount() {
        this.incrementFolkCount(1);
    }

    private void incrementGospelCount(long count) {
        this.gospel.set(this.gospel.get() + count);
    }
    public void incrementGospelCount() {
        this.incrementGospelCount(1);
    }

    private void incrementHipCount(long count) {
        this.hip.set(this.hip.get() + count);
    }
    public void incrementHipCount() {
        this.incrementHipCount(1);
    }

    private void incrementJazzCount(long count) {
        this.jazz.set(this.jazz.get() + count);
    }
    public void incrementJazzCount() {
        this.incrementJazzCount(1);
    }

    private void incrementMetalCount(long count) {
        this.metal.set(this.metal.get() + count);
    }
    public void incrementMetalCount() {
        this.incrementMetalCount(1);
    }


    public void findRegexInBody(String text){
        String cleanText = cleanLine(text);
        for (Map.Entry<String,Integer> parse: blues.entrySet()) {
            Pattern p = Pattern.compile(parse.getKey());   // the pattern to search for
            Matcher m = p.matcher(cleanText);
            if (m.find())
                incrementBluesCount();
        }
        for (Map.Entry<String,Integer> parse: classicals.entrySet()) {
            Pattern p = Pattern.compile(parse.getKey());   // the pattern to search for
            Matcher m = p.matcher(cleanText);
            if (m.find())
                incrementClassicalCount();
        }

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
//        this.words.readFields(in);

    }

    public void write(DataOutput out) throws IOException {
//        this.words.write(out);

    }


//    @Override
//    public String toString() {
//        return "termMap: " + termMap.toString()
//                +"\t" + "wordsTF: " +wordsTF.toString();
//
//    }

}
