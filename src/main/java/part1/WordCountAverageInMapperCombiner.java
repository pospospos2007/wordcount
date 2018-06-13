package part1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class WordCountAverageInMapperCombiner {
    public static class Map extends Mapper<LongWritable, Text, Text, PairWritable> {

        private HashMap<Text, PairWritable> mapper;

        protected void setup(Context context)throws IOException, InterruptedException{
            mapper = new HashMap<>();
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String u = getUID(value.toString());
            String r =getRSize(value.toString());
            if(r.equals("-")){
                return ;
            }
            Text word = new Text();
            word.set(u);
            if(mapper.containsKey(word)){
                PairWritable pair = mapper.get(word);
                pair.setKey(pair.getKey()+Integer.valueOf(r));
                pair.setValue(pair.getValue()+1);
                mapper.put(new Text(u), pair);
            }else{
                mapper.put(new Text(u), new PairWritable(Integer.valueOf(r),1));
            }

        }

        protected void cleanup(Context context)throws IOException, InterruptedException {

            for(Text word: mapper.keySet()){
                context.write(word, mapper.get(word));
            }
        }

        private static String getUID(String line){
            String[] values = line.split(" ");
            return values[0];
        }

        private static String getRSize(String line){
            String[] values = line.split(" ");
            return values[values.length-1];
        }

    }

    public static class Reduce extends Reducer<Text, PairWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<PairWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum =0,cnt = 0;
            for(PairWritable r: values){
                sum += r.getKey();
                cnt += r.getValue();
            }
            context.write(key, new IntWritable(sum/cnt));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "wordcountaverage");
        job.setJarByClass(WordCountAverageInMapperCombiner.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputValueClass(PairWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}