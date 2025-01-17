package com.example.rabbit_demo.listenerTests;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@Slf4j
@SpringBootTest
public class TransactionalRabbitAndSpring {

    @Configuration
    @EnableRabbit
    @RabbitListenerTest
    public static class Config {

        public static final String QUEUE_NAME = "example.transacted.queue";

        @Bean
        public Queue exampleTransactedQueue() {
            return  new Queue(QUEUE_NAME, true);
        }

        @Bean
        ConnectionFactory cf() {
            return new CachingConnectionFactory("localhost");
        }

        @Bean
        public RabbitAdmin admin(ConnectionFactory cf) {
            return new RabbitAdmin(cf);
        }

        @Bean
        RabbitTemplate template(ConnectionFactory cf) {
            return new RabbitTemplate(cf);
        }

        /**
         * Creates a RabbitListenerContainerFactory with channel-transacted = true.
         */
        @Bean
        public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory connectionFactory) {
            SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
            factory.setConnectionFactory(connectionFactory);

            // Enabling channel-level transactions
            factory.setChannelTransacted(true);

            return factory;
        }
    }

    @Autowired
    private RabbitTemplate rabbitTemplate;


    /**
     * This listener uses a channel-transacted container.
     * The @Transactional annotation ensures that any DB operation
     * and RabbitMQ ack/nack are in the same transaction boundary.
     */
    @Transactional
    @RabbitListener(queues = Config.QUEUE_NAME, containerFactory = "rabbitListenerContainerFactory")
    public void handleMessage(String message) {
        try {
            log.info("Received message: " + message);

            // Example DB operation (pseudo-code)
            // someRepository.save(...);

            // If everything completes without exception,
            // Spring commits BOTH the DB transaction AND the RabbitMQ channel transaction.
            // -> The message is acknowledged automatically.

            // If an exception is thrown...
            // -> Spring rolls back the RabbitMQ transaction (the message is not acked).
            // -> The message is re-queued or sent to DLX, depending on your config.

            // For demonstration, let's throw an exception on certain messages
            if ("error".equalsIgnoreCase(message)) {
                throw new RuntimeException("Simulated processing error");
            }

            log.info("Message processed successfully.");
        }
        finally {
            latch.countDown();
        }
    }

    private CountDownLatch latch = new CountDownLatch(1);

    @Test
    public void test() throws InterruptedException {
        rabbitTemplate.convertAndSend(Config.QUEUE_NAME, "error");
        latch.await(5, TimeUnit.SECONDS);
    }



}
