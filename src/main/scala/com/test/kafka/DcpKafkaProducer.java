package com.test.kafka;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;

/**
 * Created by sun on 2017/2/23.
 */
public class DcpKafkaProducer {

    private static Logger logger = LoggerFactory.getLogger(DcpKafkaProducer.class);

    public static KafkaProducer getInstance(String brokerList, String jassConfFile) {

        String krb5ConfFile =  "/etc/" + "krb5.conf";
        //String jassConfFile =  "/home/client/" + "kafka_client_jaas.conf";

        System.setProperty("java.security.krb5.conf", krb5ConfFile);
        System.setProperty("java.security.auth.login.config", jassConfFile);
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "true");
        System.setProperty("sun.security.krb5.debug", "false");
        Properties props = new Properties();

        props.put("security.protocol", SecurityProtocol.SASL_PLAINTEXT.toString());

        //props.put("zk.connect", ConsumerTest.ZK_ADDR);
        // 配置metadata.broker.list, 为了高可用, 最好配两个broker实例
        props.put("bootstrap.servers", brokerList);
        // ACK机制, 消息发送需要kafka服务端确认
//            props.put("request.required.acks", "1");

        //100M
        props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, 104857600);

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,org.apache.kafka.common.serialization.StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.StringSerializer.class);
        //props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, org.apache.kafka.common.serialization.ByteArraySerializer.class);

        System.out.println("初始化 kafka producer...");
        return new KafkaProducer(props);
    }

    public static boolean processMessageAvro(KafkaProducer producer, String topicName,  byte[] bs) throws Exception {

        List<Future> futures = new ArrayList<>();
        for (int i = 0; i < 10; i++) { // 超时重试10次
            try {
                Future<RecordMetadata> future= producer.send(new ProducerRecord<Object, Object>(topicName, bs));
                futures.add(future);
            } catch (Exception e) { // 异常后间隔3分钟重试一次
                //logger.error("send data to kafka error : " + e.getMessage(), e);
                //Thread.sleep( 5 * 1000);
                //continue;
                //TODO
                logger.error("send data to kafka error : " + e.getMessage());
                throw new RuntimeException("send data to kafka error : " + e.getMessage());
            }
            return true;
        }

        return false;

    }

    public static boolean dataToKafka(KafkaProducer kafkaProducer, String topic, List<GenericRecord> recordList)
            throws Exception {

        byte[] bs = DcpKafkaProducer.serializeAvroRecord(recordList);
        System.out.println("aaaaaa=" + recordList);
        return true;
        //return DcpKafkaProducer.processMessageAvro(kafkaProducer, topic, bs);
    }

    public static byte[] serializeAvroRecord(List<GenericRecord> records) throws Exception {


        ByteArrayOutputStream bos = null;
        try {
            bos = new ByteArrayOutputStream();
            BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(bos, null);
            GenericDatumWriter<GenericRecord> writer = null;
            for (GenericRecord genericRecord : records) {
                if (writer == null) {
                    writer = new GenericDatumWriter<GenericRecord>(genericRecord.getSchema());
                }
                writer.write(genericRecord, encoder);
            }
            encoder.flush();
            bos.close();

            byte[] serializedValue = bos.toByteArray();
            return serializedValue;
        } catch (Exception ex) {
            logger.error("", ex);
            throw ex;
        } finally {
            if (bos != null) {
                try {
                    bos.close();
                } catch (Exception e) {
                    bos = null;
                }
            }
        }
    }
}
