package com.test.kafka;

import com.test.kafka.KafkaForeachPartitionFunc;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.hive.HiveContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.ByteArrayOutputStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

public class SparkHiveKafka {

    private static Logger logger = LoggerFactory.getLogger(SparkHiveKafka.class);

    public static String testuserSchemaRecord = "{ \"namespace\": \"bjtest.hive.avro.serde\",\"name\": \"user\", \"type\": \"record\",\n" +
            "    \"fields\": [\n" +
            "        { \"name\": \"cint\", \"type\": \"int\" },\n" +
            "        { \"name\": \"cbigint\", \"type\": \"int\" },\n" +
            "        { \"name\": \"cfloat\", \"type\": \"float\" },\n" +
            "        { \"name\": \"cdouble\", \"type\": \"double\" },\n" +
            "        { \"name\": \"cboolean\", \"type\": \"boolean\" },\n" +
            "        { \"name\": \"cstring\", \"type\": \"string\" },\n" +
            "        { \"name\": \"ctinyint\", \"type\": \"int\" },\n" +
            "        { \"name\": \"csmallint\", \"type\": \"int\" },\n" +
            "        { \"name\": \"cbytes\", \"type\": \"string\" },\n" +
            "        { \"name\": \"ctime\", \"type\": \"string\" },\n" +
            "        { \"name\": \"cipv4\", \"type\": \"string\" }\n" +
            "    ]\n" +
            "}\n";

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("hive to kafka");
        SparkContext sc = new SparkContext(sparkConf);
        JavaSparkContext javaSparkContext = new JavaSparkContext(sc);
        HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);
        Dataset<Row> dataFrame = hiveContext.sql("select * from test_kafka");

        // 广播brokerlist
        final Broadcast<String> brokerListBroadcast = javaSparkContext.broadcast("DCP18:9092");
        // 广播topic
        final Broadcast<String> topicBroadcast = javaSparkContext.broadcast("hive-topic-avro");
        // 广播topicSchema
        final Broadcast<String> topicSchemaBroadcast = javaSparkContext.broadcast(SparkHiveKafka.testuserSchemaRecord);
        //广播jaasConfPath
        final Broadcast<String> jaasConfBroadcast = javaSparkContext.broadcast("/home/client/" + "kafka_client_jaas.conf");

        //生成每个parition的处理函数
        KafkaForeachPartitionFunc abstractFunction1 = new KafkaForeachPartitionFunc(topicBroadcast.getValue(), brokerListBroadcast.getValue(),
                    topicSchemaBroadcast.getValue(),jaasConfBroadcast.getValue());
        //TODO
        abstractFunction1.setBatchRecord(100);

        dataFrame.foreachPartition(abstractFunction1);

        sc.stop();
    }

    public static List<SchemaInfo> getTopicSchema( JSONArray avroArray) throws JSONException {

        List<SchemaInfo> schemaInfos = new ArrayList<>();
        for(int j = 0; j < avroArray.length(); j++) {
            JSONObject fieldInfo = (JSONObject) avroArray.get(j);
            String name = fieldInfo.get("name").toString();
            String type = fieldInfo.get("type").toString();
            if (type.contains("[")) {   // 不是基本的数据类型
                JSONArray types  = (JSONArray) fieldInfo.get("type");
                for (int i = 0; i < types.length() ; i++) {
                    if (!types.get(i).toString().equalsIgnoreCase("null")) {
                        type = types.get(i).toString();
                        break;
                    }
                }
            }

            SchemaInfo schemaInfo = new SchemaInfo();
            schemaInfo.setName(name);
            schemaInfo.setType(type);
            schemaInfos.add(schemaInfo);
        }

        return schemaInfos;
    }

    public static Object changeType(Object in, String type) {
        Object val = in;
        try {
            if (val == null || "null".equals(val.toString())) {
                return null;
            }
            if (type.toString().equals("int")) {
                val = val.equals("") ? null:Integer.parseInt(val.toString());
            } else if (type.toString().equals("boolean")) {
                val = val.equals("") ? false:Boolean.parseBoolean(val.toString());
            } else if (type.toString().equals("long")) {
                val = val.equals("") ? null:Long.parseLong(val.toString());
            } else if (type.toString().equals("float")) {
                val = val.equals("") ? null:Float.parseFloat(val.toString());
            } else if (type.toString().equals("double")) {
                val = val.equals("") ? null:Double.parseDouble(val.toString());
            } else if (type.toString().equals("string")) {
                val = val.equals("") ? null: val.toString();
            } else if (type.toString().equals("bytes")) {
                try {
                    val = ByteBuffer.wrap(val.toString().getBytes("UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    logger.error("字段类型转换异常====" + e.getMessage(), e);
                }
            }
        } catch (NumberFormatException e) {
            val = "%^&!*@#$";
            logger.error("字段类型转换异常" + e.getMessage());
            throw new RuntimeException(e);
        }
        return val;
    }


}


class SchemaInfo {

    String name;
    String type;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

}
