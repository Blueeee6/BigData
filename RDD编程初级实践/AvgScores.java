import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import scala.Tuple2;
import java.util.List;

public class AvgScores {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Duplicate").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> text1 = sc.textFile("hdfs://localhost:9000/user/spark/algorithm.txt");
        JavaRDD<String> text2 = sc.textFile("hdfs://localhost:9000/user/spark/database.txt");
        JavaRDD<String> text3 = sc.textFile("hdfs://localhost:9000/user/spark/python.txt");
        JavaRDD<String> text = text1.union(text2).union(text3);
        JavaPairRDD<String, Tuple2<Integer,Integer>> pairs = text.mapToPair(line -> {
            String[] parts = line.split(" ");
            String key = parts[0];
            Integer value = Integer.parseInt(parts[1]);
            return new Tuple2<>(key, new Tuple2<>(value,1));
        });
        JavaPairRDD<String, Tuple2<Integer, Integer>> totals = pairs.reduceByKey((a, b) ->
                new Tuple2<>(a._1 + b._1, a._2 + b._2)
        );
        JavaPairRDD<String, Double> averages = totals.mapValues(totalsAndCount ->
                (double) totalsAndCount._1 / totalsAndCount._2
        );
        List<Tuple2<String, Double>> results = averages.collect();
        for (Tuple2<String, Double> result : results) {
            System.out.println(result._1 +"平均成绩为"+ ": " + String.format("%.2f", result._2));
        }
        String outputPath = "/home/hadoop/Desktop/avgscores.txt";
        try(BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(outputPath))) {
            for(Tuple2<String,Double> line:results) {
                bufferedWriter.write(line._1 +"平均成绩为"+ ": " + String.format("%.2f", line._2));
                bufferedWriter.newLine();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
