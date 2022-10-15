package antonmry.exercise_0;

import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import antonmry.clients.producer.MockDataProducer;
import antonmry.model.Purchase;
import antonmry.util.datagen.DataGenerator;

public class KafkaStreamsIntegrationTest0 {

    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();

    private static KafkaStreamsApp0 kafkaStreamsApp;

    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String PURCHASES_TOPIC = "purchases";

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, String> transactionsInputTopic;
    private static TestOutputTopic<String, String> purchasesOutputTopic;

    @BeforeAll
    public static void setUpAll() {

        Properties properties = StreamsTestUtils.getStreamsConfig("tested",
                "127.0.0.1:1234",
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                new Properties());

        kafkaStreamsApp = new KafkaStreamsApp0(properties);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tester");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1234");
        testDriver = new TopologyTestDriver(kafkaStreamsApp.getTopology(), props);
        transactionsInputTopic = testDriver.createInputTopic(TRANSACTIONS_TOPIC, new StringSerializer(), new StringSerializer());
        purchasesOutputTopic = testDriver.createOutputTopic(PURCHASES_TOPIC, new StringDeserializer(), new StringDeserializer());
    }

    private void producePurchaseData() {

        List<Purchase> purchases = DataGenerator.generatePurchases(100, 10);
        List<String> jsonValues = MockDataProducer.convertToJson(purchases);

        jsonValues.forEach(v -> transactionsInputTopic.pipeInput(v));
    }

    /**
     * Exercise 0
     */

    @Test
    public void maskCreditCards() {

        producePurchaseData();

        List<Purchase> actualValues = MockDataProducer.convertFromJson(
                IntStream.range(0, 100)
                    .mapToObj(v -> purchasesOutputTopic.readValue()).collect(Collectors.toList()),
                Purchase.class);

        System.out.println("Received: " + actualValues);

        actualValues.forEach(v -> assertThat(
                v.getCreditCardNumber(),
                matchesPattern("xxxx-xxxx-xxxx-\\d\\d\\d\\d")
        ));
    }

}
