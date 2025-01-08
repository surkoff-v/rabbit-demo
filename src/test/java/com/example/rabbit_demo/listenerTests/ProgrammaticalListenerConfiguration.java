package com.example.rabbit_demo.listenerTests;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageListener;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.rabbit.listener.adapter.MessageListenerAdapter;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@Slf4j
@SpringBootTest
public class ProgrammaticalListenerConfiguration {

    @Configuration
    public static class conf{
        @Bean
        CachingConnectionFactory connectionFactory() {
            return new CachingConnectionFactory("localhost");
        }

        @Bean
        RabbitTemplate rabbitTemplate() {
            return new RabbitTemplate(connectionFactory());
        }

        @Bean
        Queue queue() {
            return QueueBuilder.nonDurable().autoDelete().build();
        }

        @Bean
        RabbitAdmin rabbitAdmin() {
            return new RabbitAdmin(connectionFactory());
        }
    }

    @Autowired
    CachingConnectionFactory connectionFactory;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    Queue queue;


    private SimpleMessageListenerContainer createContainer(CountDownLatch latch) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(queue.getName());
        container.setConsumerArguments(Map.of("x-priority", Integer.valueOf(1)));
        container.setMessageListener(exampleListener(latch));
        return container;
    }

    public MessageListener exampleListener(CountDownLatch latch) {
            return message -> {
                log.info("received: " + message);
                latch.countDown();
            };
    }

    @Test
    public void testDynamicQueue() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        SimpleMessageListenerContainer container = createContainer(latch);
        container.start();
        rabbitTemplate.convertAndSend(queue.getName(), "Hello, World!");
        boolean completed = latch.await(5, TimeUnit.SECONDS);
        assertTrue(completed," did not receive message");
        container.stop();
    }

}
