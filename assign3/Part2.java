import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.mapreduce.Cluster;

public class Part2 {
    
    static enum MyCounters { NUM_SAMPLES }

    public static class Part2Mapper extends Mapper<Object, Text, Text, IntWritable> {
    
        private Text gene = new Text();
        private static final double NORMAL_EXP_VAL = 0.5;
        private static final IntWritable one = new IntWritable(1);
      
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            // Split the input string into an array at each comma
            String[] result = value.toString().split(",");
            
            // Iterate through the genes and find genes that are related to the sample
            for ( int x=1; x<result.length; x++ ) {
                // Skip first element because that is the sample name
                double next_val =  Double.parseDouble(result[x]);
                if( next_val > NORMAL_EXP_VAL ) {
                    gene.set("gene_" + String.valueOf(x));
                    context.write(gene,one);
                }
            }
            // Keep track of total number of samples in a Counter
            context.getCounter(MyCounters.NUM_SAMPLES).increment(1);
        }
    }
    
    public static class Part2Reducer extends Reducer<Text,IntWritable,Text,DoubleWritable> {
        
        long num_samples;

        // Use a setup method to obtain the total sample count
        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob( context.getJobID() );
            num_samples = currentJob.getCounters().findCounter(MyCounters.NUM_SAMPLES).getValue();

        }

        private DoubleWritable result = new DoubleWritable();
        
        public void reduce(Text key, Iterable<IntWritable> values, Context context
                       ) throws IOException, InterruptedException {

            int sum = 0;
            // Sum up the number of genes related to the samples
            for (IntWritable val : values) {
                sum += val.get();
            }
            double score = (double)sum / (double)num_samples;
            result.set(score);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Part2 <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Part2");
        job.setJarByClass(Part2.class);
        job.setMapperClass(Part2Mapper.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setReducerClass(Part2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
