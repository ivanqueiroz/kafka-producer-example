package dev.ivanqueiroz.kafkaexample.main;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class TemperatureControl {

    public static void main(String[] args) {
        var consumer = new KafkaConsumer<String, String>(properties());
        consumer.subscribe(Collections.singletonList("EXEMPLO_TOPICO"));

        while (true) {
            var records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> registro : records) {
                System.out.println("------------------------------------------");
                System.out.println("Recebendo nova temperatura");
                System.out.println(registro.key());
                System.out.println(registro.value());

                final String valor = registro.value().replaceAll("º", "");
                final Integer temperatura = Integer.valueOf(valor);
                if (temperatura > 30) {
                    System.out.println("Está calor");
                } else if (temperatura < 20) {
                    System.out.println("Está frio");
                }

                System.out.println("Temperatura processada.");
            }
        }
    }

    private static Properties properties() {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, TemperatureControl.class.getName());
        return properties;
    }
}
