import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Part1 {

  public static class Part1Mapper 
       extends Mapper<Object, Text, Text, DoubleWritable>{
    
    private Text sample = new Text();
    private DoubleWritable gene = new DoubleWritable();
      
    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
        String[] result = value.toString().split(",");
        sample.set(result[0]);
        for ( int x=1; x<result.length ; x++ ) {
            gene.set( Double.parseDouble(result[x]) );
            context.write(sample, gene);
        }

    }
  }
  
  public static class Part1Reducer 
       extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
    private DoubleWritable result = new DoubleWritable(0);

    public void reduce(Text key, Iterable<DoubleWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
    
        for (DoubleWritable val : values) {
            if( val.get() > result.get() ) {
                result.set( val.get() );
            }
        }
        context.write(key, result);
        result.set(0);
        }
    }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
      System.err.println("Usage: Part1 <in> <out>");
      System.exit(2);
    }
    Job job = new Job(conf, "Part1");
    job.setJarByClass(Part1.class);
    job.setMapperClass(Part1Mapper.class);
    job.setCombinerClass(Part1Reducer.class);
  //  job.setReducerClass(Part1Reducer.class);
    job.setOutputKeyClass(Text.class);
    //job.setOutputValueClass(Text.class);
    job.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
