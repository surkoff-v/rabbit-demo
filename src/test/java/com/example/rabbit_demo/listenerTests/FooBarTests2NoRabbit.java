package com.example.rabbit_demo.listenerTests;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import org.junit.jupiter.api.Test;
import org.springframework.amqp.core.AnonymousQueue;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.annotation.EnableRabbit;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.junit.RabbitAvailable;
import org.springframework.amqp.rabbit.test.RabbitListenerTest;
import org.springframework.amqp.rabbit.test.RabbitListenerTestHarness;
import org.springframework.amqp.rabbit.test.TestRabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willReturn;
import static org.mockito.Mockito.mock;
import static org.springframework.amqp.rabbit.test.RabbitListenerTestHarness.InvocationData;

@SpringBootTest
public class FooBarTests2NoRabbit {

    @Configuration
    @EnableRabbit
    @RabbitListenerTest(spy = true, capture = true)
    public static class ListenerConfig {
        @Bean
        public Listener listener() {
            return new Listener();
        }

        @Bean
        public ConnectionFactory connectionFactory() throws IOException {
            ConnectionFactory factory = mock(ConnectionFactory.class);
            Connection connection = mock(Connection.class);
            Channel channel = mock(Channel.class);
            AMQP.Queue.DeclareOk declareOk = mock(AMQP.Queue.DeclareOk.class);
            willReturn(connection).given(factory).createConnection();
            willReturn(channel).given(connection).createChannel(anyBoolean());
            given(channel.isOpen()).willReturn(true);
            given(channel.queueDeclare(anyString(), anyBoolean(), anyBoolean(), anyBoolean(), anyMap()))
                    .willReturn(declareOk);
            return factory;
        }

        @Bean
        public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory() throws IOException {
            SimpleRabbitListenerContainerFactory factory = new SimpleRabbitListenerContainerFactory();
            factory.setConnectionFactory(connectionFactory());
            return factory;
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
        public TestRabbitTemplate template() throws IOException {
            return new TestRabbitTemplate(connectionFactory());
        }

        @Bean
        public RabbitAdmin admin(ConnectionFactory cf) {
            return new RabbitAdmin(cf);
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

        InvocationData invocationData =
                this.harness.getNextInvocationDataFor("foo", 0, TimeUnit.SECONDS);
        assertThat(invocationData.getArguments()[0]).isEqualTo("foo");
        assertThat((String) invocationData.getResult()).isEqualTo("FOO");
    }

    @Test
    public void testOneWay() throws Exception {
        this.rabbitTemplate.convertAndSend(this.queue2.getName(), "bar");
        this.rabbitTemplate.convertAndSend(this.queue2.getName(), "baz");
        this.rabbitTemplate.convertAndSend(this.queue2.getName(), "ex");

        InvocationData invocationData =
                this.harness.getNextInvocationDataFor("bar", 10, TimeUnit.SECONDS);
        Object[] args = invocationData.getArguments();
        assertThat((String) args[0]).isEqualTo("bar");
        assertThat((String) args[1]).isEqualTo(queue2.getName());

        invocationData = this.harness.getNextInvocationDataFor("bar", 10, TimeUnit.SECONDS);
        args = invocationData.getArguments();
        assertThat((String) args[0]).isEqualTo("baz");

        invocationData = this.harness.getNextInvocationDataFor("bar", 10, TimeUnit.SECONDS);
        args = invocationData.getArguments();
        assertThat((String) args[0]).isEqualTo("ex");
        assertEquals("ex", invocationData.getThrowable().getMessage());
    }


   public static class Listener {
       private boolean failed;

       @RabbitListener(id="foo", queues="#{queue1.name}")
       public String foo(String foo) {
           return foo.toUpperCase();
       }

       @RabbitListener(id="bar", queues="#{queue2.name}")
       public void foo1(@Payload String foo) {
           if (!failed && foo.equals("ex")) {
               failed = true;
               throw new RuntimeException(foo);
           }
           failed = false;

       }
   }

}
