package com.example.rabbit_demo;

import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.springframework.amqp.rabbit.test.mockito.LambdaAnswer;
import org.springframework.amqp.rabbit.test.mockito.LatchCountDownAndCallRealMethodAnswer;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

@SpringBootTest
class RabbitDemoApplicationTests {

	@Test
	void answerTest() {
		Thing delegat = new Thing();
		Thing thing = spy(new Thing());

		doAnswer(new LambdaAnswer<String>(true, (i, r) -> r + r,delegat))
				.when(thing).thing(anyString());
		assertEquals("THINGTHING", thing.thing("thing"));

		doAnswer(new LambdaAnswer<String>(true, new LambdaAnswer.ValueToReturn<String>() {
            @Override
            public String apply(InvocationOnMock i, String r) {
                return r + i.getArguments()[0];
            }
        }, delegat))
				.when(thing).thing(anyString());
		assertEquals("THINGthing", thing.thing("thing"));

		doAnswer(new LambdaAnswer<String>(true, (i, r) ->
				"" + i.getArguments()[0] + i.getArguments()[0],delegat)).when(thing).thing(anyString());
		assertEquals("thingthing", thing.thing("thing"));
	}

}

 class Thing {
	public String thing(String thing) {
		return thing.toUpperCase();
	}

}