package com.example.rabbit_demo.listenerTests;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AcknowledgeMode;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.ListenerContainerConsumerFailedEvent;
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.event.EventListener;
import org.springframework.amqp.core.Queue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


@Slf4j
@SpringBootTest
@Disabled("Disabled until we can figure out how to test this properly")
public class CatchListenerContainerConsumerFailedEvent {

    public static CountDownLatch latch = new CountDownLatch(1);

    private static class ConsumerFailureEventListener {
            @EventListener
            public void handleConsumerFailure(Object event) {
                log.info("Event received: " + event.getClass().getName());
            }
    }

    private static class Listener {
        @RabbitListener(id="foo", queues="#{queue1.name}")
        public void listen(String message) {
            throw new RuntimeException("Simulated consumer failure");
        }
    }

    @Configuration
    @EnableRabbit
    @RabbitListenerTest
    public static class Conf {
        @Bean
        public ConsumerFailureEventListener consumerFailureEventListener() {
               return new ConsumerFailureEventListener();
        }
        @Bean
        public Listener Listener() {
            return new Listener();
        }
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
            containerFactory.setApplicationEventPublisher(applicationEvent -> {
                // Publish events properly
            });
            containerFactory.setDefaultRequeueRejected(false);

            return containerFactory;
        }
    }

    @Autowired
    RabbitTemplate template;

    @Autowired
    Queue queue1;

    @Test
    public void test() throws InterruptedException {
        template.convertAndSend(this.queue1.getName(),"foo");
        latch.await(5, TimeUnit.SECONDS);
    }
}
