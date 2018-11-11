package edu.usfca.cs.mr.wordcount.Analysis_03;

import org.apache.hadoop.io.*;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;


public class ReadableWriter implements Writable {

    private ArrayWritable keyWritableList = new ArrayWritable(Text.class);
    private ArrayWritable termWritableList = new ArrayWritable(DoubleWritable.class);

    private List<Text> keyList = new LinkedList<>();
    private List<DoubleWritable> termList = new LinkedList<>();

    private Map<String, Double> termCountMap = new HashMap<>();

    private Long words = 0L;

    public ReadableWriter() {
        keyWritableList = new ArrayWritable(Text.class);
        termWritableList = new ArrayWritable(DoubleWritable.class);
        keyList = new LinkedList<>();
        termList = new LinkedList<>();
        words = 0L;
        termCountMap = new HashMap<>();
    }

    public ReadableWriter(String text) {
        this.countTerm(text);
        this.updateTermMap(text);
    }


    public ArrayWritable getKeyList() {
        return keyWritableList;
    }

    public ArrayWritable getTermList() {
        return termWritableList;
    }


    private void updateTermMap(String text) {
        String cleanText = cleanLine(text);
        String[] word = cleanText.split(" ");
        for (String w : word) {
            if (w.length() > 0) {
                double count = this.termCountMap.get(w);
                if (count > 0) {
                    keyList.add(new Text(w));
                    termList.add(new DoubleWritable(count));
                }
            }
        }
        Text[] keyListT = new Text[keyList.size()];
        DoubleWritable[] termListD = new DoubleWritable[termList.size()];
        for (int i = 0; i < keyList.size(); i++) {
            keyListT[i] = keyList.get(i);
            termListD[i] = termList.get(i);
        }
        keyWritableList.set(keyListT);
        termWritableList.set(termListD);
    }

    private void countTerm(String text) {
        String cleanText = cleanLine(text);
        String[] word = cleanText.split(" ");
        if (word.length > 0)
            for (String w : word) {
                if (w.length() > 0) {
                    words++;
                    if (!termCountMap.containsKey(w)) {
                        termCountMap.put(w, 1D);
                    } else {
                        double count = termCountMap.get(w);
                        termCountMap.put(w, count + 1D);
                    }
                }
            }
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

    public boolean hasCount() {
        return this.words > 0
                && this.keyList.size() > 0
                && this.termList.size() > 0;
    }

    private double round(double d, int decimalPlace) {
        // see the Javadoc about why we use a String in the constructor
        // http://java.sun.com/j2se/1.5.0/docs/api/java/math/BigDecimal.html#BigDecimal(double)
        BigDecimal bd = new BigDecimal(Double.toString(d));
        bd = bd.setScale(decimalPlace, BigDecimal.ROUND_HALF_UP);
        return bd.doubleValue();
    }


    public void readFields(DataInput in) throws IOException {
        this.keyWritableList.readFields(in);
        this.termWritableList.readFields(in);

//        this.termMap.readFields(in);
//        this.wordsTF.readFields(in);

    }

    public void write(DataOutput out) throws IOException {
        this.keyWritableList.write(out);
        this.termWritableList.write(out);

//        this.termMap.write(out);
//        this.wordsTF.write(out);
    }


    @Override
    public String toString() {
        return "keyWritableList: " + keyWritableList.toString()
                + "\t" + "termWritableList: " + termWritableList.toString();

    }

}
