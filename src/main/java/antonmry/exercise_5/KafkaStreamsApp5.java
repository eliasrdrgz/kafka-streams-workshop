package antonmry.exercise_5;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.StreamJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import antonmry.exercise_5.joiner.PurchaseJoiner;
import antonmry.exercise_5.partitioner.RewardsStreamPartitioner;
import antonmry.exercise_5.transformer.PurchaseRewardTransformer;
import antonmry.model.CorrelatedPurchase;
import antonmry.model.Purchase;
import antonmry.model.PurchasePattern;
import antonmry.model.RewardAccumulator;
import antonmry.util.serde.StreamsSerdes;

public class KafkaStreamsApp5 {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaStreamsApp5.class);

    public Topology getTopology() {
        return topology;
    }

    private final Topology topology;

    private KafkaStreams kafkaStreams;

    public KafkaStreamsApp5(Properties properties) {

        Serde<Purchase> purchaseSerde = StreamsSerdes.PurchaseSerde();
        Serde<String> stringSerde = Serdes.String();

        Serde<PurchasePattern> purchasePatternSerde = StreamsSerdes.PurchasePatternSerde();
        Serde<RewardAccumulator> rewardAccumulatorSerde = StreamsSerdes.RewardAccumulatorSerde();

        Serde<CorrelatedPurchase> correlatedPurchaseSerde = new StreamsSerdes.CorrelatedPurchaseSerde();

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, Purchase> transactionKStream = streamsBuilder
                .stream("transactions", Consumed.with(stringSerde, purchaseSerde));

        KStream<String, Purchase> purchaseKStream = transactionKStream
                .mapValues(p -> Purchase.builder(p).maskCreditCard().build());

        KStream<String, PurchasePattern> patternKStream = purchaseKStream
                .mapValues(purchase -> PurchasePattern.builder(purchase).build());

        patternKStream.to("patterns", Produced.with(stringSerde, purchasePatternSerde));

        KeyValueMapper<String, Purchase, Long> purchaseDateAsKey = (key, purchase) -> purchase
                .getPurchaseDate().getTime();

        KStream<Long, Purchase> filteredKStream = purchaseKStream
                .filter((key, purchase) -> purchase.getPrice() > 5.00)
                .selectKey(purchaseDateAsKey);

        filteredKStream.to("purchases", Produced.with(Serdes.Long(), purchaseSerde));

        String rewardsStateStoreName = "rewardsPointsStore";
        RewardsStreamPartitioner streamPartitioner = new RewardsStreamPartitioner();

        KeyValueBytesStoreSupplier storeSupplier = Stores.inMemoryKeyValueStore(rewardsStateStoreName);

        StoreBuilder<KeyValueStore<String, Integer>> storeBuilder = Stores
                .keyValueStoreBuilder(storeSupplier, Serdes.String(), Serdes.Integer());

        streamsBuilder.addStateStore(storeBuilder);

        KStream<String, Purchase> transByCustomerStream = purchaseKStream
                .through("customer_transactions", Produced.with(stringSerde, purchaseSerde, streamPartitioner));

        KStream<String, RewardAccumulator> statefulRewardAccumulator = transByCustomerStream.
                transformValues(() -> new PurchaseRewardTransformer(rewardsStateStoreName),
                        rewardsStateStoreName);

        statefulRewardAccumulator.to("rewards", Produced.with(stringSerde, rewardAccumulatorSerde));

        Predicate<String, Purchase> isShoe = (key, purchase) -> purchase.getDepartment()
                .equalsIgnoreCase("shoes");

        Predicate<String, Purchase> isFragrance = (key, purchase) -> purchase.getDepartment()
                .equalsIgnoreCase("fragrance");

        KStream<String, Purchase>[] branchesStream = purchaseKStream
                .selectKey((k, v) -> v.getCustomerId())
                .branch(isShoe, isFragrance);

        KStream<String, Purchase> shoeStream = branchesStream[0];
        KStream<String, Purchase> fragranceStream = branchesStream[1];

        shoeStream.to("shoes", Produced.with(stringSerde, purchaseSerde));
        fragranceStream.to("fragrances", Produced.with(stringSerde, purchaseSerde));

        ValueJoiner<Purchase, Purchase, CorrelatedPurchase> purchaseJoiner = new PurchaseJoiner();

        JoinWindows twentyMinuteWindow = JoinWindows.of(Duration.ofMinutes(20));

        KStream<String, CorrelatedPurchase> joinedKStream = shoeStream.join(fragranceStream,
                purchaseJoiner,
                twentyMinuteWindow,
                StreamJoined.with(stringSerde,
                        purchaseSerde,
                        purchaseSerde));

        joinedKStream.to("shoesAndFragrancesAlerts", Produced.with(stringSerde, correlatedPurchaseSerde));

        // TODO: ingest the purchaseKStream to a new topic "customer_detection"

        // TODO: create a new in Memory KeyValue Store with name "customers"

        // TODO: create the new KTable based in the previous topic and event store
        //  See https://kafka.apache.org/10/javadoc/org/apache/kafka/streams/kstream/KTable.html

        // TODO (Homework): expose the State Store using a REST API

        this.topology = streamsBuilder.build();
        this.kafkaStreams = new KafkaStreams(topology, properties);
    }

    public KeyValueIterator<String, Purchase> getCustomersTableRecords() {

        // TODO: create a ReadOnlyKeyValueStore to read from the Event Store "customers"

        // TODO: return all the records
        return null;
    }

    void start() {

        LOG.info("Kafka Streams Application Started");
        kafkaStreams.start();
    }

    void stop() {
        LOG.info("Shutting down the Kafka Streams Application now");
        kafkaStreams.close();
    }

    public static void main(String[] args) throws Exception {
        KafkaStreamsApp5 kafkaStreamsApp = new KafkaStreamsApp5(getProperties());
        kafkaStreamsApp.start();
        Thread.sleep(65000);
        kafkaStreamsApp.stop();
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.CLIENT_ID_CONFIG, "Kafka-Streams-Client");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "purchases");
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "Kafka-Streams-App");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }

}