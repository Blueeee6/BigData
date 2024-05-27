import org.apache.spark.sql.*;

public class Transfer {
    public static void main(String[] args) {
        // 创建SparkSession
        SparkSession spark = SparkSession.builder().appName("TransferApp").master("local").getOrCreate();
        // 从本地txt文件读取数据并创建DataFrame
        Dataset<Row> df = spark.read().textFile("file:///home/hadoop/employee.txt").toDF();
        df = df.selectExpr("split(value, ',')[0] as id", "split(value, ',')[1] as name", "split(value, ',')[2] as age");
        // 将DataFrame注册为一个临时表
        df.createOrReplaceTempView("employee");
        // 执行SQL查询
        Dataset<Row> result = spark.sql("SELECT concat('id:', id, ',name:', name, ',age:', age) AS result FROM employee");
        result.show(false);
        // 关闭SparkSession
        spark.close();
    }
}

