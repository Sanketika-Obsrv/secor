package com.pinterest.secor.reader;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.common.ZookeeperConnector;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.util.IdUtil;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.UnknownHostException;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;

// TODO: choose better name
public class KafkaClientMessageIterator implements KafkaMessageIterator {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaClientMessageIterator.class);
    private KafkaConsumer<byte[], byte[]> mKafkaConsumer;
    private Queue<ConsumerRecord<byte[], byte[]>> mRecordsBatch;
    private ZookeeperConnector mZookeeperConnector;

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Message next() {
        if (mRecordsBatch.isEmpty()) {
            mKafkaConsumer.poll(Duration.ofMillis(1000)).forEach(mRecordsBatch::add);
        }

        ConsumerRecord<byte[], byte[]> consumerRecord = mRecordsBatch.remove();
        return new Message(consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(),
                consumerRecord.key(), consumerRecord.value(), consumerRecord.timestamp());
    }

    @Override
    public void init(SecorConfig config) throws UnknownHostException {
        Properties props = new Properties();
        props.put("group.id", config.getKafkaGroup());
        props.put("enable.auto.commit", false);
        // TODO: This does nothing for now
        props.put("auto.offset.reset", "earliest");
        // TODO: Check if old consumer.timeout.ms is equivalent as this
        props.put("request.timeout.ms", config.getConsumerTimeoutMs());
        props.put("client.id", IdUtil.getConsumerId());
        props.put("fetch.min.bytes", config.getFetchMinBytes());
        props.put("fetch.max.bytes", config.getFetchMaxBytes());
        props.put("key.deserializer", ByteArrayDeserializer.class);
        props.put("value.deserializer", ByteArrayDeserializer.class);

        optionalConfig(config.getSocketReceiveBufferBytes(), conf -> props.put("receive.buffer.bytes", conf));
        optionalConfig(config.getFetchMinBytes(), conf -> props.put("fetch.min.bytes", conf));
        optionalConfig(config.getFetchMaxBytes(), conf -> props.put("fetch.max.bytes", conf));

        mZookeeperConnector = new ZookeeperConnector(config);
        mRecordsBatch = new LinkedList<>();
        mKafkaConsumer = new KafkaConsumer<>(props);
        mKafkaConsumer.subscribe(Pattern.compile(config.getKafkaTopicFilter()), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                // TODO: Validate if secor needs something done here
                for (TopicPartition topicPartition : collection) {
                    LOG.debug("Topic partition {} revoked from consumer", topicPartition);
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                try {
                    Map<TopicPartition, Long> committedOffsets = getCommittedOffsets(collection);
                    committedOffsets.forEach(((topicPartition, offset) -> {
                        LOG.debug("Seeking {} to offset {}", topicPartition, offset);
                        mKafkaConsumer.seek(topicPartition, Math.max(0, offset));
                    }));
                } catch (Exception e) {
                    LOG.trace("Unable to fetch committed offsets from zookeeper", e);
                    throw new RuntimeException(e);
                }
            }
        });
    }

    private Map<TopicPartition, Long> getCommittedOffsets(Collection<TopicPartition> assignment) throws Exception {
        Map<TopicPartition, Long> committedOffsets = new HashMap<>();

        for (TopicPartition topicPartition : assignment) {
            com.pinterest.secor.common.TopicPartition secorTopicPartition = new com.pinterest.secor.common.TopicPartition(topicPartition.topic(), topicPartition.partition());
            long committedOffset = mZookeeperConnector.getCommittedOffsetCount(secorTopicPartition);
            committedOffsets.put(topicPartition, committedOffset);
        }

        return committedOffsets;
    }

    @Override
    public void commit() {
        // TODO: Confirm that this will never work, since secor logic won't permit this commit after a rebalance
    }

    private void optionalConfig(String maybeConf, Consumer<String> configConsumer) {
        Optional.ofNullable(maybeConf).filter(String::isEmpty).ifPresent(configConsumer);
    }
}
