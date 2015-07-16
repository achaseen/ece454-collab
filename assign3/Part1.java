import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Part1 {

    public static class Part1Mapper extends Mapper<Object, Text, Text, Text> {
    
        // The name of the sample, used as the key in the output
        private Text sample = new Text();
        // The gene as a double. We only store the largest gene found
        private double max_gene = 0;
        // Itermediate list used to aggregate list of max genes
        private String int_list = "gene_1";
        // The list of genes to output as the value
        private Text gene_list = new Text();
      
        public void map(Object key, Text value, Context context
                        ) throws IOException, InterruptedException {
            // Split the input string into an array at each comma
            String[] result = value.toString().split(",");
            // The first element in the array is the sample name
            sample.set(result[0]);
            
            // Iterate through the genes and find the biggest
            for ( int x=2; x<result.length ; x++ ) {
                double next_val =  Double.parseDouble(result[x]);
                if( next_val > max_gene ) {
                    max_gene = next_val;
                    int_list = "gene_" + String.valueOf(x);
                } else if (next_val == max_gene) {
                    // If there are two genes that are both max, add it to the list
                    int_list = int_list + ",gene_" + String.valueOf(x);
                }
            }
            gene_list.set(int_list);
            context.write(sample, gene_list);
            // Reset trackers
            max_gene=0;
            int_list="gene_1";
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: Part1 <in> <out>");
            System.exit(2);
        }
        Job job = new Job(conf, "Part1");
        job.setJarByClass(Part1.class);
        job.setMapperClass(Part1Mapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
