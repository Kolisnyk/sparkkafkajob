/**
 * Created by oleksii on 11.01.17.
 */

    import kafka.javaapi.producer.Producer;
    import kafka.producer.KeyedMessage;
    import kafka.producer.ProducerConfig;

    import java.util.Properties;

    class KafkaConnection {
        private static Producer<Integer, String> producer;
        private final Properties properties = new Properties();

        KafkaConnection() {
            properties.put("metadata.broker.list", "localhost:9092");
            properties.put("serializer.class", "kafka.serializer.StringEncoder");
            properties.put("request.required.acks", "1");
            producer = new Producer<>(new ProducerConfig(properties));
        }

        static void main(String topic, String message) {
            KeyedMessage<Integer, String> data = new KeyedMessage<>(topic, message);
            producer.send(data);
            producer.close();
        }
    }

