package part2;

/**
 * Created by Wenqiang on 6/13/18.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;

public class PairApproach{

    public static class Map extends Mapper<LongWritable, Text, PairWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elements = line.split(" ");
            for(int w = 0; w<elements.length; w++){
                for(int u = w+1; u<elements.length; u++){
                    if(!elements[u].equalsIgnoreCase(elements[w])){
                        PairWritable pair1 = new PairWritable(elements[w],elements[u]);
                        context.write(pair1,one);
                        PairWritable pair2 = new PairWritable(elements[w],"*");
                        context.write(pair2, one);
                    }else break;

                }
            }
        }

    }

    public static class Reduce extends
            Reducer<PairWritable, IntWritable, PairWritable, DoubleWritable> {

        private double N;

        protected void setup(Context context)throws IOException, InterruptedException{
            N = 0;
        }

        public void reduce(PairWritable key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            double sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            if(key.getValue().toString().equals("*")){
                N = sum;
            }else{
                context.write(key, new DoubleWritable(sum/N));
            }

        }
    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();

        Job job = new Job(conf, "pairapproach");
        job.setJarByClass(PairApproach.class);

        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(PairWritable.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}
