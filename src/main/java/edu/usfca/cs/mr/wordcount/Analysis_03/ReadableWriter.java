package edu.usfca.cs.mr.wordcount.Analysis_03;

import org.apache.hadoop.io.*;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class ReadableWriter implements Writable {
//    private Map<String, Double> wordsTF = new HashMap<>();
//    private Map<String, Double> termMap = new HashMap<>();
    private MapWritable wordsTF =new MapWritable();
    private MapWritable termMap =new MapWritable();
    private LongWritable words;

    public ReadableWriter(){
    }

    public ReadableWriter(String text){
        this.words = new LongWritable(this.getNumberOfWords(text));
        this.updateTermMap(text);
    }

    public MapWritable getTermMap(){
        return termMap;
    }

    public MapWritable getTFMap(){
        return wordsTF;
    }

    public void Copy (ReadableWriter rw) {
        this.wordsTF = rw.wordsTF;
        this.termMap = rw.termMap;

    }

    private void updateTermMap(String text){
        String cleanText = cleanLine(text);
        String[] word = cleanText.split(" ");
        for (String w : word) {
            long count = this.getCurrentWordCount(w,text);
            if(!w.equals("") && count >0) {
                termMap.put(new Text(w), new LongWritable(count));
                if (words.get() != 0) {
                    double tf = count / words.get();
                    wordsTF.put(new Text(w), new DoubleWritable(tf));
                }
            }
        }
    }



    private int getCurrentWordCount(String token, String text){
        String cleanText = cleanLine(text);
        String[] word = cleanText.split(" ");
        int words = 0;
        for (String w : word) {
            if (w.length() > 0 &&(w.equals(token)))
                words++;
        }
        return words;
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
        this.termMap.readFields(in);
        this.wordsTF.readFields(in);
    }

    public void write(DataOutput out) throws IOException {
//        this.words.write(out);

        this.termMap.write(out);
        this.wordsTF.write(out);
    }


    @Override
    public String toString() {
        return "termMap: " + termMap.toString()
                +"\t" + "wordsTF: " +wordsTF.toString();

    }

}
