import java.io.IOException;
import java.util.StringTokenizer;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Cluster;

public class Part3 {

    public static class Part3Mapper1 extends Mapper<Object, Text, Text, Text> {
    
        private Text gene = new Text();
        private Text values = new Text();

        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
                // Split the input string into an array at each comma
                String[] genes = value.toString().split(",");
                for(int i = 1; i < genes.length; i++) {
                    if(Double.parseDouble(genes[i]) > 0) {
                        //gene = "gene_index"
                        gene.set("gene_" + String.valueOf(i));
                        //values = "sample_index,<gene_inex value>"
                        values.set(genes[0] + "," + genes[i]);
                        context.write(gene, values);
                    }
                }
        }
    }
    
    public static class Part3Reducer1 extends Reducer<Text,Text,Text,DoubleWritable> {

        private Text sample_pair = new Text();
        private DoubleWritable similarity = new DoubleWritable();

        private String[] s1value;
        private String[] s2value;
        private int index1 = 0;
        private int index2 = 0;
        
        public void reduce(Text key, Iterable<Text> values, Context context
                       ) throws IOException, InterruptedException {
            ArrayList<String> samples = new ArrayList();

            for(Text value: values) {
                for(int i = 0; i < samples.size(); i++) {
                    s1value = samples.get(i).split(",");
                    s2value = value.toString().split(",");
                    index1 = Integer.parseInt(s1value[0].split("_")[1]);
                    index2 = Integer.parseInt(s2value[0].split("_")[1]);
                    if(index1 < index2) 
                        //key = "sample_first_index,sample_second_index"
                        sample_pair.set(s1value[0] + "," + s2value[0]);
                    else
                        //key = "sample_first_index,sample_second_index"
                        sample_pair.set(s2value[0] + "," + s1value[0]);
                    //result = s1gene_index * s2gene_index
                    similarity.set(Double.parseDouble(s1value[1]) * Double.parseDouble(s2value[1]));
                    context.write(sample_pair, similarity);
                }
                samples.add(value.toString());
            }
        }
    }
    
    public static class Part3Mapper2 extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private Text sample_pair = new Text();
        private DoubleWritable similarity = new DoubleWritable();

        public void map(LongWritable key, Text value, Context context
                        ) throws IOException, InterruptedException {
            String[] values = value.toString().split(",");
            sample_pair.set(values[0] + "," + values[1]);
            similarity.set(Double.parseDouble(values[2]));
            context.write(sample_pair, similarity);
        }
    }
    

    public static class Part3Reducer2 extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {

        private DoubleWritable similarity = new DoubleWritable();
        
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context
                       ) throws IOException, InterruptedException {
            double sum = 0;
            for(DoubleWritable value: values) {
                sum += value.get();
            }
            if(sum > 0) {
            similarity.set(sum);
            context.write(key, similarity);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        conf1.set("mapreduce.output.textoutputformat.separator", ",");
        String[] otherArgs = new GenericOptionsParser(conf1, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Part3 <in> <out>");
            System.exit(2);
        }
        Job job1 = new Job(conf1, "Part3_1");
        job1.setJarByClass(Part3.class);
        job1.setMapperClass(Part3Mapper1.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setReducerClass(Part3Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job1, new Path("intermediate_output"));
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        conf2.set("mapreduce.output.textoutputformat.separator", ",");
        Job job2 = new Job(conf2, "Part3_2");
        job2.setJarByClass(Part3.class);
        job2.setMapperClass(Part3Mapper2.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setReducerClass(Part3Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job2, new Path("intermediate_output"));
        FileOutputFormat.setOutputPath(job2, new Path(otherArgs[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}