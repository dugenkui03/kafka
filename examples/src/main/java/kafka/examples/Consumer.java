/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import kafka.utils.ShutdownableThread;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

// java类还能继承 scala 类
public class Consumer extends ShutdownableThread {
    // 组合的方式包装 消费者 对象
    private final KafkaConsumer<Integer, String> consumer;

    /**
     * 主题队列、组ID
     *
     * 传送给消费这的数据大小？剩余的消息大小？
     */
    private final String topic;
    private final String groupId;
    private final int numMessageToConsume;
    private int messageRemaining;
    private final CountDownLatch latch;

    public Consumer(final String topic,
                    final String groupId,
                    final Optional<String> instanceId,
                    final boolean readCommitted,
                    final int numMessageToConsume,
                    final CountDownLatch latch) {
        // ？？队列名称，是否支持中断？？
        super("KafkaConsumerExample", false);
        this.groupId = groupId;

        //配置
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        instanceId.ifPresent(id -> props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        //？？隔离级别：读取提交？？
        if (readCommitted) {
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // fixme 配置消费者
        consumer = new KafkaConsumer<>(props);

        this.topic = topic;
        this.numMessageToConsume = numMessageToConsume;
        this.messageRemaining = numMessageToConsume;
        this.latch = latch;
    }

    KafkaConsumer<Integer, String> get() {
        return consumer;
    }

    @Override
    public void doWork() {
        // fixme 消费者使用指定的配置、订阅topic
        consumer.subscribe(Collections.singletonList(this.topic));

        // 获取记录
        ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));

        //消费记录：包含分区信息、记录 k-v、偏移量；
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println(
                    groupId + " received message : from partition "
                            + record.partition()
                            + ", (" + record.key()
                            + ", " + record.value()
                            + ") at offset "
                            + record.offset()
            );
        }

        // 所有topic的记录数量递减
        messageRemaining -= records.count();

        // 没有数据的时候：打印结束消费
        if (messageRemaining <= 0) {
            System.out.println(groupId + " finished reading " + numMessageToConsume + " messages");
            // 调用指定次数的时候，会释放所有在此latch等待的线程
            latch.countDown();
        }
    }

    @Override
    public String name() {
        return null;
    }

    // 是否中断
    @Override
    public boolean isInterruptible() {
        return false;
    }
}
