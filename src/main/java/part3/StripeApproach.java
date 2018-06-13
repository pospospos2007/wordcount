package part3;

/**
 * Created by Wenqiang on 6/13/18.
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;

public class StripeApproach {

    public static class Map extends Mapper<LongWritable, Text, Text, MapWritable> {

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] elements = line.split(" ");
            for(int w = 0; w<elements.length; w++){
                HashMap<Text,IntWritable> mapper = new HashMap<>();
                for(int u = w+1; u<elements.length; u++){
                    if(!elements[u].equalsIgnoreCase(elements[w])){
                        Text word = new Text();
                        word.set(elements[u]);
                        if(mapper.containsKey(word)){
                            IntWritable times = new IntWritable(mapper.get(word).get()+1);
                            mapper.put(word, times);
                        }else{
                            mapper.put(word, new IntWritable(1));
                        }
                    }else break;

                }

                MapWritable mapWritable = new MapWritable();
                for (Entry<Text,IntWritable> entry : mapper.entrySet()) {
                    if(null != entry.getKey() && null != entry.getValue()){
                        mapWritable.put(entry.getKey(),entry.getValue());
                    }
                }
                context.write(new Text(elements[w]), mapWritable);
            }
        }

    }

    public static class Reduce extends Reducer<Text, MapWritable, PairWritable, DoubleWritable> {

        public void reduce(Text key, Iterable<MapWritable>values, Context context)
                throws IOException, InterruptedException {
            HashMap<Text,DoubleWritable> h = new HashMap<>();
            double cnt = 0;
            for (MapWritable val : values) {
                for(Writable w: val.keySet()){
                    Text t = (Text)w;
                    IntWritable iw= (IntWritable)val.get(t);
                    DoubleWritable dw = (DoubleWritable)h.get(t);
                    if(h.containsKey(t)){
                        h.put(t, new DoubleWritable(dw.get()+iw.get()));
                    }else{
                        h.put(t, new DoubleWritable(iw.get()));
                    }
                    cnt+=iw.get();
                }
            }

            for(Text k: h.keySet()){
                PairWritable pair = new PairWritable(key.toString(),k.toString());
                context.write(pair, new DoubleWritable(h.get(k).get()/cnt));
            }
        }


    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "stripeapproach");
        job.setJarByClass(StripeApproach.class);

        job.setOutputKeyClass(PairWritable.class);
        job.setOutputValueClass(DoubleWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MapWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }

}