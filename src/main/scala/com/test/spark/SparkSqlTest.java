package com.test.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SparkSqlTest {

	static SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("MultiDataSource");
	static JavaSparkContext sc = new JavaSparkContext(sparkConf);
	static SQLContext sqlsc = new SQLContext(sc);


	public static Dataset<Row> getLogDf() {
		List<String> userAccessLog = new ArrayList();
		userAccessLog.add("zhangsan,123435");
		userAccessLog.add("zhangsan,211123422");
		userAccessLog.add("zhangsan,32341234");
		userAccessLog.add("zhangsan,41805208126370281");
		userAccessLog.add("zhangsan,1820150000003959");
		userAccessLog.add("lisi,1817430219307158");
		userAccessLog.add("lisi,1234");
		userAccessLog.add("lisi,124124234");
		userAccessLog.add("lisi,4351");
		sc.parallelize(userAccessLog);
		JavaRDD<String> userAccessLogRDD = sc.parallelize(userAccessLog, 5);

		// 首先，将普通的RDD，转换为元素为Row的RDD
		JavaRDD<Row> userAccessLogRowRDD = userAccessLogRDD
				.map(new Function<String, Row>() {

					@Override
					public Row call(String line) throws Exception {
						String[] split = line.split(",");
						return RowFactory.create(split[0], Long.valueOf(split[1]));
					}

				});

		// 构造DataFrame的元数据
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("length", DataTypes.LongType, true));

		/**
		 * 2、构建StructType用于DataFrame 元数据的描述
		 *
		 */
		StructType structType = DataTypes.createStructType(structFields);
		/**
		 * 3、基于MeataData以及RDD<Row>来构造DataFrame
		 */
		Dataset<Row> logDF = sqlsc
				.createDataFrame(userAccessLogRowRDD, structType);


        return logDF;
	}

    public static void testUnion() {
        Dataset<Row> logDF = getLogDf();

		logDF.registerTempTable("log");


		//except all 不支持,except支持
        Dataset<Row> dataResults = sqlsc.sql("select * from log where  name ='zhangsan' except select * from log where  name ='zhangsan'");

		//intersect支持，intersect all不支持
		//DataFrame dataResults = sqlsc.sql("select * from log where  name ='zhangsan' intersect select * from log where  name ='zhangsan'");

		//error
		//DataFrame dataResults = sqlsc.sql("select * from log where  name ='zhangsan' intercept  select * from log where  name ='zhangsan'");
        dataResults.show();
	}

	public static void testJoin() {

        Dataset<Row> logDF = getLogDf();

		/**
		 * 4、注册成为临时表以供后续的SQL查询操作
		 */
		logDF.registerTempTable("log");

        Dataset<Row> dataResults = sqlsc.sql("select * from log where  name ='zhangsan'");
		/**
		 * 6对结果进行处理，包括由DataFrame转换为RDD<Row> 以及结果的持久化
		 */
		List<Row> collect = dataResults.javaRDD().collect();
		for (Row lists : collect) {
			System.out.println(lists);
		}

		//String url = "jdbc:oracle:thin:nwd/123456@//localhost:1521/orcl";
	    String url="jdbc:mysql://10.1.234.25:3306/testkafka?user=dcp&password=Dcp@1234";

		Properties prop = new Properties();

		// 使用SQLContext创建jdbc的DataFrame
        Dataset<Row> dbDf = sqlsc.read().jdbc(url, "accounts", prop);

		dbDf.registerTempTable("user");

		    //rdd注册的临时表join上关系库的表
        Dataset<Row> joinResults= sqlsc.sql("select user.id, user.name, log.length  from user left join log   on user.name=log.name ");

		collect = joinResults.javaRDD().collect();
		for (Row lists : collect) {
			System.out.println(lists);
		}


		/** another example */
		//  Dataset<Row> df = sqlContext .read() .format("jdbc") .option("url", JDBCURL) .option("dbtable", "tsys_user").load();
		// df.printSchema();
		// Counts people by age
		//Dataset<Row> countsByAge = df.groupBy("customStyle").count();
		//countsByAge.show();
		//Saves countsByAge to S3 in the JSON format.
		//countsByAge.write().format("json").save("hdfs://192.168.1.17:9000/administrator/sql-result" + sdf.format(new Date()));
	}

	public static void main(String[] args) {
        testUnion();
	}
}
