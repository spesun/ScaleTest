package com.test.spark;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
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


	public static DataFrame getLogDf() {
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

		// ���ȣ�����ͨ��RDD��ת��ΪԪ��ΪRow��RDD
		JavaRDD<Row> userAccessLogRowRDD = userAccessLogRDD
				.map(new Function<String, Row>() {

					@Override
					public Row call(String line) throws Exception {
						String[] split = line.split(",");
						return RowFactory.create(split[0], Long.valueOf(split[1]));
					}

				});

		// ����DataFrame��Ԫ����
		List<StructField> structFields = new ArrayList<StructField>();
		structFields.add(DataTypes.createStructField("name", DataTypes.StringType, true));
		structFields.add(DataTypes.createStructField("length", DataTypes.LongType, true));

		/**
		 * 2������StructType����DataFrame Ԫ���ݵ�����
		 *
		 */
		StructType structType = DataTypes.createStructType(structFields);
		/**
		 * 3������MeataData�Լ�RDD<Row>������DataFrame
		 */
		DataFrame logDF = sqlsc
				.createDataFrame(userAccessLogRowRDD, structType);


        return logDF;
	}

    public static void testUnion() {
		DataFrame logDF = getLogDf();

		logDF.registerTempTable("log");


		//except all ��֧��,except֧��
		DataFrame dataResults = sqlsc.sql("select * from log where  name ='zhangsan' except select * from log where  name ='zhangsan'");

		//intersect֧�֣�intersect all��֧��
		//DataFrame dataResults = sqlsc.sql("select * from log where  name ='zhangsan' intersect select * from log where  name ='zhangsan'");

		//error
		//DataFrame dataResults = sqlsc.sql("select * from log where  name ='zhangsan' intercept  select * from log where  name ='zhangsan'");
        dataResults.show();
	}

	public static void testJoin() {

        DataFrame logDF = getLogDf();

		/**
		 * 4��ע���Ϊ��ʱ���Թ�������SQL��ѯ����
		 */
		logDF.registerTempTable("log");

		DataFrame dataResults = sqlsc.sql("select * from log where  name ='zhangsan'");
		/**
		 * 6�Խ�����д���������DataFrameת��ΪRDD<Row> �Լ�����ĳ־û�
		 */
		List<Row> collect = dataResults.javaRDD().collect();
		for (Row lists : collect) {
			System.out.println(lists);
		}

		//String url = "jdbc:oracle:thin:nwd/123456@//localhost:1521/orcl";
	    String url="jdbc:mysql://10.1.234.25:3306/testkafka?user=dcp&password=Dcp@1234";

		Properties prop = new Properties();

		// ʹ��SQLContext����jdbc��DataFrame
		DataFrame dbDf = sqlsc.read().jdbc(url, "accounts", prop);

		dbDf.registerTempTable("user");

		    //rddע�����ʱ��join�Ϲ�ϵ��ı�
		DataFrame  joinResults= sqlsc.sql("select user.id, user.name, log.length  from user left join log   on user.name=log.name ");

		collect = joinResults.javaRDD().collect();
		for (Row lists : collect) {
			System.out.println(lists);
		}


		/** another example */
		//  DataFrame df = sqlContext .read() .format("jdbc") .option("url", JDBCURL) .option("dbtable", "tsys_user").load();
		// df.printSchema();
		// Counts people by age
		//DataFrame countsByAge = df.groupBy("customStyle").count();
		//countsByAge.show();
		//Saves countsByAge to S3 in the JSON format.
		//countsByAge.write().format("json").save("hdfs://192.168.1.17:9000/administrator/sql-result" + sdf.format(new Date()));
	}

	public static void main(String[] args) {
        testUnion();
	}
}
