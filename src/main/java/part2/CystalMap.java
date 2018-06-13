package part2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public  class CystalMap extends Mapper<LongWritable, Text, Text, IntWritable> {

        private Logger logger = Logger.getLogger(CystalMap.class);

       private final static IntWritable one = new IntWritable(1);

       private Text word = new Text();

       public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           String line = value.toString();
           StringTokenizer tokenizer = new StringTokenizer(line);
           List<String> list =new ArrayList<String>();
           while (tokenizer.hasMoreTokens()) {
               list.add(tokenizer.nextToken());
           }
           for(int i=0;i<list.size()-1;i++){
               List<String> subList = new ArrayList<String>();
               subList =  neighbors(i,list);
               for(int j=0;j<subList.size();j++){
                   Pair pair = new Pair(list.get(i),list.get(j));
                   logger.info("(("+list.get(i)+","+list.get(j)+"),"+"1)");
                   word.set(pair.toString());
                   context.write(word,one);
               }
           }
    }

    public List<String> neighbors(int index, List<String> list){
        List<String> nList = new ArrayList<String>();
        for(int i=index+1;i<list.size();i++){
            if(!list.get(index).equals(list.get(i))){
                nList.add(list.get(i));
            }
        }
        return nList;
    }
}
