package com.balaji;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    private static void findPaths(int total, List<Integer> numbers, List<Integer> acc){
        if(total==0){
            System.out.println(acc);
        } else{

                for(int i:numbers) {
                    if(total-i >= 0) {
                        List<Integer> x =new ArrayList<>();
                                x.addAll(acc);
                        x.add(i);
                        findPaths(total - i, numbers, x);
                    }
                }
            }

    }

    static int minCostPath = -1;

    private static void findShortestPath(int[][] input, List<Integer> result, int x, int y){
        if(x > 2 || y >2)
            return;
        if(x==2 && y==2){
            for(int n:result){
                System.out.print(n);
            }
            System.out.println();
        }else{
            List<Integer> newResult = new ArrayList<>();
            newResult.addAll(result);
            newResult.add(input[x][y]);
            findShortestPath(input, newResult, x+1, y);
            findShortestPath(input, newResult, x, y+1);
        }
    }

    public static void main(String[] args){
        findPaths(13, Arrays.asList(3,5,10), new ArrayList<>());
    }
    public static void main2(String[] args){
        int[][] input = {{0,1,2},{3,4,5},{6,7,8}};
        findShortestPath(input, new ArrayList<>(), 0,0);
    }

    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable>{

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            System.out.println("Value =================== "+value);
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }

    }

    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum) ;
            context.write(key, result);
        }
    }

    public static void main1(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setInputFormatClass(XmlInputFormat.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("/tmp/input/data.xml"));
        FileOutputFormat.setOutputPath(job, new Path("/tmp/output-2"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}