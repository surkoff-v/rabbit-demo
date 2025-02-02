package com.example.rabbit_demo.listenerTests;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.amqp.rabbit.test.RabbitListenerTestHarness;
import org.springframework.amqp.rabbit.test.mockito.LatchCountDownAndCallRealMethodAnswer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.verify;

@SpringBootTest
public class FooBarTests {

    @Configuration
    @EnableRabbit
    @RabbitListenerTest
    public static class ListenerConfig {
        @Bean
        public Listener Listener() {
            return new Listener();
        }

        @Bean
        public ConnectionFactory connectionFactory() {
            return new CachingConnectionFactory("localhost");
        }

        @Bean
        public Queue queue1() {
            return new AnonymousQueue();
        }

        @Bean
        public Queue queue2() {
            return new AnonymousQueue();
        }

        @Bean
        public RabbitTemplate template(ConnectionFactory cf) {
            return new RabbitTemplate(cf);
        }

        @Bean
        public RabbitAdmin admin(ConnectionFactory cf) {
            return new RabbitAdmin(cf);
        }

        @Bean
        public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(ConnectionFactory cf) {
            SimpleRabbitListenerContainerFactory containerFactory = new SimpleRabbitListenerContainerFactory();
            containerFactory.setConnectionFactory(cf);
            return containerFactory;
        }
    }

    @Autowired
    private RabbitListenerTestHarness harness;

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Autowired
    private Queue queue1;

    @Autowired
    private Queue queue2;


    @Test
    public void testTwoWay() throws Exception {
        assertEquals("FOO", this.rabbitTemplate.convertSendAndReceive(this.queue1.getName(), "foo"));

        Listener listener = this.harness.getSpy("foo");
        assertNotNull(listener);
        // was called 1 time
        verify(listener).foo("foo");
    }

    @Test
    public void testOneWay() throws Exception {
        Listener listener = this.harness.getSpy("bar");
        assertNotNull(listener);

        LatchCountDownAndCallRealMethodAnswer answer = this.harness.getLatchAnswerFor("bar", 2);
        doAnswer(answer).when(listener).foo(anyString(), anyString());

        this.rabbitTemplate.convertAndSend(this.queue2.getName(), "bar");
        this.rabbitTemplate.convertAndSend(this.queue2.getName(), "baz");

        assertTrue(answer.await(10));
        verify(listener).foo("bar", this.queue2.getName());
        verify(listener).foo("baz", this.queue2.getName());
    }

    @Slf4j
    public static class Listener {
        @RabbitListener(id="foo", queues="#{queue1.name}")
        public String foo(String foo) {
            return foo.toUpperCase();
        }

        @RabbitListener(id="bar", queues="#{queue2.name}")
        public void foo(@Payload String foo, @Header("amqp_receivedRoutingKey") String rk) {
            log.info("Received message: {}, routing key: {}", foo, rk);

        }
    }
}
