
import java.io.IOException;

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

public class Sort {
    private static int num=0;
    public static class Map extends Mapper<Object, Text, IntWritable, IntWritable>{

        private static IntWritable data = new IntWritable();
        public void map(Object key, Text value, Context context) throws IOException,InterruptedException{
            String text = value.toString();
            data.set(Integer.parseInt(text));
            context.write(data, new IntWritable(1));
            num++;
        }
    }


    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>{
        private static IntWritable line_num = new IntWritable(num);

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{
                context.write(line_num, key);
                line_num = new IntWritable(line_num.get() - 1);

        }
    }
    public static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: wordcount <in><out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf,"Sort");//实例化Sort类
        job.setSortComparatorClass(IntWritableDecreasingComparator.class);
        job.setJarByClass(Sort.class);//设置主类名
        job.setMapperClass(Map.class);//指定使用上述代码自定义的Map类
        job.setReducerClass(Reduce.class);//指定使用上述代码自定义的Reduce类
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);//设定Reduce类输出的<K,V>,V类型
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));//添加输入文件位置
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));//设置输出结果文件位置
        System.exit(job.waitForCompletion(true)?0:1);

    }
}


