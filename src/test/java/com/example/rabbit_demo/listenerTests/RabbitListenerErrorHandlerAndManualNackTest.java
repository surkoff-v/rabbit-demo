package com.example.rabbit_demo.listenerTests;


import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.api.RabbitListenerErrorHandler;
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.amqp.support.AmqpHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import com.rabbitmq.client.Channel;

@SpringBootTest
@Slf4j
public class RabbitListenerErrorHandlerAndManualNackTest {

    @Configuration
    @EnableRabbit
    @RabbitListenerTest
    public static class Conf {
        @Bean
        Queue queue1() {
            return new AnonymousQueue();
        }

        @Bean
        ConnectionFactory connectionFactory() {
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

        @Bean
        public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory cf) {
            SimpleRabbitListenerContainerFactory containerFactory = new SimpleRabbitListenerContainerFactory();
            containerFactory.setConnectionFactory(cf);
            containerFactory.setMismatchedQueuesFatal(false);
            containerFactory.setAcknowledgeMode(AcknowledgeMode.MANUAL);
            containerFactory.setDefaultRequeueRejected(false);
            return containerFactory;
        }

        @Bean
        RabbitListenerErrorHandler errorHandler() {
            return (amqpMessage, message, exception) -> {
                log.error("Handling error: " + exception.getMessage());
                /**
                 * we can do there manual nack
                 */
                message.getHeaders().get(AmqpHeaders.CHANNEL, Channel.class)
                  .basicReject(message.getHeaders().get(AmqpHeaders.DELIVERY_TAG, Long.class),
                               false);
                latch.countDown();
                return null;
            };

        }
    }

    @RabbitListener(queues = "#{queue1.name}")
    public void listen(String in) {
        log.info("Message: " + in);
        latch.countDown();
        throw new RuntimeException("Planned");
    }

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    Queue queue1;

    static CountDownLatch latch = new CountDownLatch(1);

    @Test
    public void test() throws InterruptedException {
        rabbitTemplate.convertAndSend(queue1.getName(), "foo");
        latch.await(5, TimeUnit.SECONDS);
    }

}
