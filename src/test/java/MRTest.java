import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import PackageDemo.WordCount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;


public class MRTest {
    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordCount.MapForWordCount mapper = new WordCount.MapForWordCount();
        WordCount.ReduceForWordCount reducer = new WordCount.ReduceForWordCount();
        mapDriver = MapDriver.newMapDriver(mapper);
        reduceDriver = ReduceDriver.newReduceDriver(reducer);
        mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
    }

    @Test
    public void testMapper() {
        final LongWritable key = new LongWritable(0);
        mapDriver.withInput(key, new Text(
                "hello"));
        mapDriver.withOutput(new Text("h"), new IntWritable(1));
        mapDriver.withOutput(new Text("e"), new IntWritable(1));
        mapDriver.withOutput(new Text("l"), new IntWritable(1));
        mapDriver.withOutput(new Text("l"), new IntWritable(1));
        mapDriver.withOutput(new Text("o"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testReducer() {
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        reduceDriver.withInput(new Text("l"), values);
        reduceDriver.withOutput(new Text("l"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testMapReduce() throws Exception {
        mapReduceDriver.withInput(new LongWritable(), new Text(
                "hello"));
        List<IntWritable> values = new ArrayList<IntWritable>();
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        values.add(new IntWritable(1));
        mapReduceDriver.withOutput(new Text("e"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("h"), new IntWritable(1));
        mapReduceDriver.withOutput(new Text("l"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("o"), new IntWritable(1));
        mapReduceDriver.runTest();
    }
}