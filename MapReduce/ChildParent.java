
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ChildParent {
    private static int time = 0;//用于表头输出一次
    public static class Map extends Mapper<Object,Text,Text,Text> {
        public void map(Object key,Text value,Context context) throws InterruptedException,IOException {
            String line = value.toString();
            String[] parts = line.split("\\s+");
            if(!parts[0].equals("child")&&!parts[1].equals("parent")) {
                context.write(new Text(parts[0]), new Text("c"+"+"+parts[0]+"+"+parts[1])); // 孩子-父母关系
                context.write(new Text(parts[1]), new Text("p"+"+"+parts[0]+"+"+parts[1])); // 父母-孩子关系
            }
        }

    }
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            if (time == 0) {
                context.write(new Text("grandchild"), new Text("grandparent"));
                time++;
            }
            ArrayList<String> grandchild = new ArrayList<>();
            ArrayList<String> grandparent = new ArrayList<>();
            for(Text value:values) {
                String tmp[] = value.toString().split("\\+");
                if(tmp[0].equals("c")) {
                    grandparent.add(tmp[2]);
                }
                else {
                    grandchild.add(tmp[1]);
                }
            }
            for(String text1:grandchild){
                for(String text2:grandparent){
                    context.write(new Text(text1), new Text(text2));
                    //输出结果
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in><out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf,"ChildParent");//实例化Sort类
        job.setJarByClass(ChildParent.class);//设置主类名
        job.setMapperClass(Map.class);//指定使用上述代码自定义的Map类
        job.setReducerClass(Reduce.class);//指定使用上述代码自定义的Reduce类
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);//设定Reduce类输出的<K,V>,V类型
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//添加输入文件位置
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//设置输出结果文件位置
        System.exit(job.waitForCompletion(true)?0:1);

    }
}

