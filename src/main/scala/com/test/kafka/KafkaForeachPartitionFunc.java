package com.test.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.spark.sql.Row;
import org.json.JSONArray;
import org.json.JSONObject;
import scala.collection.Iterator;
import scala.runtime.AbstractFunction1;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

public class KafkaForeachPartitionFunc extends AbstractFunction1<Iterator<Row>, BoxedUnit> implements Serializable {

    String topicName;
    String brokerList;
    String topicSchema;
    String kafkaJaasConf;

    int batchRecord = 100;

    public KafkaForeachPartitionFunc(String topicName, String brokerList, String topicSchema, String kafkaJaasConf) {
        this.topicName = topicName;
        this.brokerList = brokerList;
        this.topicSchema = topicSchema;
        this.kafkaJaasConf = kafkaJaasConf;
    }

    public int getBatchRecord() {
        return batchRecord;
    }

    public void setBatchRecord(int batchRecord) {
        this.batchRecord = batchRecord;
    }


    @Override
    public BoxedUnit apply(Iterator<Row> it) {
        try {
            call(it);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return BoxedUnit.UNIT;
    }

    public void call(Iterator<Row> it) throws Exception {

        List<SchemaInfo> schemaInfos = null;
        JSONObject json = new JSONObject(this.topicSchema);
        JSONArray avroArrayTmp = (JSONArray) json.get("fields");
        schemaInfos = SparkHiveKafka.getTopicSchema(avroArrayTmp);

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(this.topicSchema);

        // kafka producer
        KafkaProducer kafkaProducer = DcpKafkaProducer.getInstance(this.brokerList, this.kafkaJaasConf);

        List<GenericRecord> recordList = new ArrayList<GenericRecord>();
        while (it.hasNext()){
            Row row = it.next();
            //Future<RecordMetadata> future= kafkaProducer.send(new ProducerRecord<Object, Object>(this.topicName, row.toString()));

            GenericRecord record = new GenericData.Record(schema);
            for (SchemaInfo schemaInfo: schemaInfos) {
                String name = schemaInfo.getName();
                String type = schemaInfo.getType();

                Object val = row.getAs(name);
                Object newVal = SparkHiveKafka.changeType(val, type);
                record.put(schemaInfo.getName(), newVal);

            }

            recordList.add(record);
            if (recordList.size() == this.getBatchRecord()) { // 达到记录条数，写入kafka
                // 文件内容写入kafka
                Boolean kafkaFlag = DcpKafkaProducer.dataToKafka(kafkaProducer, this.topicName, recordList);
                recordList.clear(); // 清空recordList，重新计数
            }
        }

        if (recordList.size() != 0) {
            Boolean kafkaFlag = DcpKafkaProducer.dataToKafka(kafkaProducer, this.topicName, recordList);
        }

        System.out.println("aaaaa= " + recordList.size());
        kafkaProducer.close();
    }
}
