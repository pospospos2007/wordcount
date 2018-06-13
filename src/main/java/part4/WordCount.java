package part4;

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
import java.util.HashMap;

public class WordCount {

    public static class Map extends
            Mapper<LongWritable, Text, PairWritable, IntWritable> {

        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] elements = line.split(" ");
            for (int w = 0; w < elements.length; w++) {
                for (int u = w + 1; u < elements.length; u++) {
                    if (!elements[u].equalsIgnoreCase(elements[w])) {
                        PairWritable pair1 = new PairWritable(elements[w],
                                elements[u]);
                        context.write(pair1, one);
                    } else
                        break;

                }
            }
        }

    }

    public static class Reduce extends
            Reducer<PairWritable, IntWritable, PairWritable, DoubleWritable> {

        private int N;
        private String wprev;
        private HashMap<Text, DoubleWritable> hybrid;

        protected void setup(Context context) throws IOException,
                InterruptedException {
            N = 0;
            wprev = null;
            hybrid = new HashMap<>();
        }

        public void reduce(PairWritable key, Iterable<IntWritable> values,
                           Context context) throws IOException, InterruptedException {
            String w = key.getKey().toString();
            String u = key.getValue().toString();

            if(wprev != null && !w.equals(wprev)){
                sumMapAndDivide(context);
                hybrid = new HashMap<>();
            }

            wprev = w;
            Text tu = new Text(u);
            for(IntWritable iw: values){
                if(hybrid.containsKey(tu)){
                    hybrid.put(tu, new DoubleWritable(hybrid.get(tu).get()+1));
                }else{
                    hybrid.put(tu, new DoubleWritable(1.0));
                }

            }

        }

        protected void cleanup(Context context)throws IOException, InterruptedException {

            sumMapAndDivide(context);
        }

        private void sumMapAndDivide(Context context) throws IOException, InterruptedException{
            double sum = 0;
            for(Text t: hybrid.keySet()){
                sum += hybrid.get(t).get();
            }
            for(Text t: hybrid.keySet()){
                PairWritable pair = new PairWritable(wprev, t.toString());
                context.write(pair, new DoubleWritable(hybrid.get(t).get()/sum));

            }
        }

    }

    public static void main(String[] args) throws Exception {


        Configuration conf = new Configuration();

        Job job = new Job(conf, "hybridapproach");
        job.setJarByClass(WordCount.class);

        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
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