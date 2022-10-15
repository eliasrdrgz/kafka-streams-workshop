package antonmry.exercise_5;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesPattern;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.StreamsTestUtils;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import antonmry.clients.producer.MockDataProducer;
import antonmry.model.CorrelatedPurchase;
import antonmry.model.Purchase;
import antonmry.model.PurchasePattern;
import antonmry.model.RewardAccumulator;
import antonmry.util.datagen.DataGenerator;

public class KafkaStreamsIntegrationTest5 {

    private static final String STRING_SERDE_CLASSNAME = Serdes.String().getClass().getName();

    private static KafkaStreamsApp5 kafkaStreamsApp;

    private static final String TRANSACTIONS_TOPIC = "transactions";
    private static final String PURCHASES_TOPIC = "purchases";
    private static final String PATTERNS_TOPIC = "patterns";
    private static final String PURCHASES_TABLE_TOPIC = "customer_detection";
    private static final String REWARDS_TOPIC = "rewards";
    private static final String SHOES_TOPIC = "shoes";
    private static final String FRAGRANCES_TOPIC = "fragrances";
    private static final String SHOES_AND_FRAGANCES_TOPIC = "shoesAndFragrancesAlerts";

    private static TopologyTestDriver testDriver;
    private static TestInputTopic<String, String> transactionsInputTopic;
    private static TestOutputTopic<String, String> purchasesOutputTopic;
    private static TestOutputTopic<String, String> patternsOutputTopic;
    private static TestOutputTopic<String, String> shoesOutputTopic;
    private static TestOutputTopic<String, String> fragancesOutputTopic;
    private static TestOutputTopic<String, String> rewardsOutputTopic;
    private static TestOutputTopic<String, String> shoesAndFragancesOutputTopic;
    private static TestOutputTopic<String, String> purchasesTableOutputTopic;

    @BeforeAll
    public static void setUpAll() {

        Properties properties = StreamsTestUtils.getStreamsConfig("tested",
                "127.0.0.1:1234",
                STRING_SERDE_CLASSNAME,
                STRING_SERDE_CLASSNAME,
                new Properties());

        kafkaStreamsApp = new KafkaStreamsApp5(properties);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "tester");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:1234");
        testDriver = new TopologyTestDriver(kafkaStreamsApp.getTopology(), props);
        transactionsInputTopic = testDriver.createInputTopic(TRANSACTIONS_TOPIC, new StringSerializer(), new StringSerializer());
        purchasesOutputTopic = testDriver.createOutputTopic(PURCHASES_TOPIC, new StringDeserializer(), new StringDeserializer());
        patternsOutputTopic = testDriver.createOutputTopic(PATTERNS_TOPIC, new StringDeserializer(), new StringDeserializer());
        shoesOutputTopic = testDriver.createOutputTopic(SHOES_TOPIC, new StringDeserializer(), new StringDeserializer());
        fragancesOutputTopic = testDriver.createOutputTopic(FRAGRANCES_TOPIC, new StringDeserializer(), new StringDeserializer());
        rewardsOutputTopic = testDriver.createOutputTopic(REWARDS_TOPIC, new StringDeserializer(), new StringDeserializer());
        shoesAndFragancesOutputTopic = testDriver.createOutputTopic(SHOES_AND_FRAGANCES_TOPIC, new StringDeserializer(), new StringDeserializer());
        purchasesTableOutputTopic = testDriver.createOutputTopic(PURCHASES_TABLE_TOPIC, new StringDeserializer(), new StringDeserializer());
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

    /**
     * Exercise 1
     */

    @Test
    public void testPurchasePatterns() {

        producePurchaseData();

        List<PurchasePattern> actualValues = MockDataProducer.convertFromJson(
                IntStream.range(0, 100)
                    .mapToObj(v -> patternsOutputTopic.readValue()).collect(Collectors.toList()),
                PurchasePattern.class);

        System.out.println(PATTERNS_TOPIC + " received: " + actualValues);

        actualValues.forEach(v -> assertThat(
                v.getZipCode(), not(emptyOrNullString())));
        actualValues.forEach(v -> assertThat(
                v.getItem(), not(emptyOrNullString())));
        actualValues.forEach(v -> assertThat(
                v.getAmount(), greaterThan(0.0)));
    }


    /**
     * Exercise 2
     */

    @Test
    public void maskCreditCardsAndFilterSmallPurchases() {

        producePurchaseData();

        List<Purchase> actualValues = MockDataProducer.convertFromJson(
                IntStream.range(0, 10)
                    .mapToObj(v -> purchasesOutputTopic.readValue()).collect(Collectors.toList()),
                Purchase.class);

        System.out.println(PURCHASES_TOPIC + " received: " + actualValues);

        actualValues.forEach(v -> assertThat(
                v.getCreditCardNumber(),
                matchesPattern("xxxx-xxxx-xxxx-\\d\\d\\d\\d")
        ));

        actualValues.forEach(v -> assertThat(
                v.getPrice(),
                greaterThan(5.0)
        ));
    }

    @Test
    public void branchShoesAndFragrances() {

        producePurchaseData();

        List<Purchase> shoesValues = MockDataProducer.convertFromJson(
                IntStream.range(0, 10)
                    .mapToObj(v -> shoesOutputTopic.readValue()).collect(Collectors.toList()),
                Purchase.class);

        System.out.println(SHOES_TOPIC + " received: " + shoesValues);

        List<Purchase> fragrancesValues = MockDataProducer.convertFromJson(
                IntStream.range(0, 10)
                    .mapToObj(v -> fragancesOutputTopic.readValue()).collect(Collectors.toList()),
                Purchase.class);

        System.out.println(FRAGRANCES_TOPIC + " received: " + fragrancesValues);

        assertThat("number of shoes", (long) shoesValues.size(), greaterThan(0L));

        shoesValues.forEach(v -> assertThat(
                v.getDepartment(),
                equalToIgnoringCase("shoes"))
        );

        assertThat("number of fragrances", (long) fragrancesValues.size(), greaterThan(0L));

        fragrancesValues.forEach(v -> assertThat(
                v.getDepartment(),
                equalToIgnoringCase("fragrance"))
        );

    }

    /**
     * Exercise 3
     */

    @Test
    public void testRewardsAccumulator() {

        producePurchaseData();

        List<RewardAccumulator> actualValues = MockDataProducer.convertFromJson(
                IntStream.range(0, 10)
                    .mapToObj(v -> rewardsOutputTopic.readValue()).collect(Collectors.toList()),
                RewardAccumulator.class);

        System.out.println(REWARDS_TOPIC + " received: " + actualValues);

        actualValues.forEach(v -> assertThat(
                v.getCustomerId(),
                matchesPattern(".*,.*")));

        actualValues.forEach(v -> assertThat(
                v.getPurchaseTotal(), greaterThan(0.0)
        ));

        actualValues.forEach(v -> assertThat(
                v.getCurrentRewardPoints(), greaterThan(0)
        ));

        assertThat(actualValues.stream().filter(v -> v.getCurrentRewardPoints() < v.getTotalRewardPoints()).count(),
                greaterThan(1L));
    }

    /**
     * Exercise 4
     */

    @Test
    public void joinShoesAndFragances() {

        producePurchaseData();

        List<CorrelatedPurchase> actualValues = MockDataProducer.convertFromJson(
                IntStream.range(0, 10)
                    .mapToObj(v -> shoesAndFragancesOutputTopic.readValue()).collect(Collectors.toList()),
                CorrelatedPurchase.class);

        System.out.println(SHOES_AND_FRAGANCES_TOPIC + " received: " + actualValues);
        System.out.println(SHOES_AND_FRAGANCES_TOPIC + " count: " + (long) actualValues.size());

        assertThat((long) actualValues.size(), greaterThan(1L));

        actualValues.forEach(v -> assertThat(
                v.getCustomerId(),
                notNullValue())
        );

        actualValues.forEach(v -> assertThat(
                v.getFirstPurchaseTime(),
                notNullValue())
        );

        actualValues.forEach(v -> assertThat(
                v.getSecondPurchaseTime(),
                notNullValue())
        );

        actualValues.forEach(v -> assertThat(
                (long) v.getItemsPurchased().size(),
                greaterThan(1L))
        );
    }

    /**
     * Exercise 5
     */

    @Test
    public void testQueryableKTable() {

        producePurchaseData();

        List<Purchase> actualValues = MockDataProducer.convertFromJson(
                IntStream.range(0, 10)
                    .mapToObj(v -> purchasesOutputTopic.readValue()).collect(Collectors.toList()),
                Purchase.class);

        System.out.println(PURCHASES_TOPIC + " received: " + actualValues);
        System.out.println(PURCHASES_TOPIC + " count: " + (long) actualValues.size());

        List<Purchase> tableValues = MockDataProducer.convertFromJson(
                IntStream.range(0, 10)
                    .mapToObj(v -> purchasesTableOutputTopic.readValue()).collect(Collectors.toList()),
                Purchase.class);

        System.out.println(PURCHASES_TABLE_TOPIC + " received: " + tableValues);
        System.out.println(PURCHASES_TABLE_TOPIC + " count: " + (long) tableValues.size());

        KeyValueStore<String, Purchase> store = testDriver.getKeyValueStore("customers");
        KeyValueIterator<String, Purchase> range = store.all();

        int total = 0;
        System.out.print("customers event store keys: ");
        while (range.hasNext()) {
            KeyValue<String, Purchase> next = range.next();
            System.out.print(next.key + ", ");
            total++;
        }
        assertThat(total, is(greaterThan(0)));
    }

    @Test
    public void printTopology() {
        // Utility test in case you would like to print the topology using https://zz85.github.io/kafka-streams-viz/
        System.out.println(kafkaStreamsApp.getTopology().describe().toString());
    }
}
