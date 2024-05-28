import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class Duplicate {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Duplicate").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> text1 = sc.textFile("hdfs://localhost:9000/user/hadoop/input/test/wordfile1.txt");
        JavaRDD<String> text2 = sc.textFile("hdfs://localhost:9000/user/hadoop/input/test/wordfile2.txt");
        JavaRDD<String> combinedText = text1.union(text2);
        JavaRDD<String> uniqueLines = combinedText.distinct();
        for (String str : uniqueLines.collect()) {
            System.out.println(str);
        }
        List<String> uniqueList = uniqueLines.collect();

        // 保存结果到本地文件
        String outputPath = "/home/hadoop/Desktop/uniqueLines.txt";
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath))) {
            for (String line : uniqueList) {
                writer.write(line);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        sc.close();


    }
}
