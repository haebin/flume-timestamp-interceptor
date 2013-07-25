/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flume.interceptor;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.interceptor.EventTimestampInterceptor.Constants;
import org.junit.Test;

import com.google.common.base.Charsets;

/**
 * @author Frederick Haebin Na (haebin.na@gmail.com)
 */
public class EventTimestampInterceptorTest {
	/**
	 * Ensure that the "timestamp" header gets set (to something)
	 */
	@Test
	public void testBasic() throws Exception {
		String dateFormat = "###[yyyy-MM-dd HH:mm:ss]";

		Context context = new Context();
		context.put(Constants.DELIMITER, "|");
		context.put(Constants.INDEX, "0");
		context.put(Constants.FORMAT, dateFormat);
		context.put(Constants.PRESERVE, "false");

		Interceptor.Builder builder = new EventTimestampInterceptor.Builder();
		builder.configure(context);
		Interceptor interceptor = builder.build();

		Event event = EventBuilder.withBody(
				"###[1979-07-21 00:00:00]|test event", Charsets.UTF_8);
		assertThat(event.getHeaders().get(Constants.TIMESTAMP), is(nullValue()));

		event = interceptor.intercept(event);
		String timestampStr = event.getHeaders().get(Constants.TIMESTAMP);
		long timestamp = Long.parseLong(timestampStr);

		SimpleDateFormat dateFormatter = new SimpleDateFormat(dateFormat);
		String formattedDate = dateFormatter.format(new Date(timestamp));
		System.out.println(formattedDate);
		assertThat(formattedDate, is("###[1979-07-21 00:00:00]"));
	}

	/**
	 * Ensure timestamp is NOT overwritten when preserveExistingTimestamp ==
	 * true
	 */
	@Test
	public void testPreserve() throws ClassNotFoundException,
			InstantiationException, IllegalAccessException {

		Context context = new Context();
		context.put(Constants.PRESERVE, "true");

		Interceptor.Builder builder = new EventTimestampInterceptor.Builder();
		builder.configure(context);
		Interceptor interceptor = builder.build();

		long originalTs = 1L;
		Event event = EventBuilder.withBody("1979-07-21 00:00:00|test event",
				Charsets.UTF_8);
		event.getHeaders().put(Constants.TIMESTAMP, Long.toString(originalTs));

		event = interceptor.intercept(event);
		String timestampStr = event.getHeaders().get(Constants.TIMESTAMP);
		System.out.println(timestampStr);
		assertThat(Long.parseLong(timestampStr), is(originalTs));
	}

	/**
	 * Ensure timestamp IS overwritten when preserveExistingTimestamp == false
	 */
	@Test
	public void testClobber() throws ClassNotFoundException,
			InstantiationException, IllegalAccessException {

		Context context = new Context();
		context.put(Constants.PRESERVE, "false"); // DEFAULT behavior

		Interceptor.Builder builder = new EventTimestampInterceptor.Builder();
		builder.configure(context);
		Interceptor interceptor = builder.build();

		long originalTs = 1L;
		Event event = EventBuilder.withBody("1979-07-21 00:00:00|test event",
				Charsets.UTF_8);
		event.getHeaders().put(Constants.TIMESTAMP, Long.toString(originalTs));

		event = interceptor.intercept(event);
		String timestampStr = event.getHeaders().get(Constants.TIMESTAMP);
		System.out.println(timestampStr);
		assertThat(timestampStr, not("1"));
	}

	/**
	 * Ensure timestamp IS overwritten with system time when date format is
	 * invalid.
	 */
	@Test
	public void testBadDateFormate() throws ClassNotFoundException,
			InstantiationException, IllegalAccessException {

		Interceptor.Builder builder = new EventTimestampInterceptor.Builder();
		Interceptor interceptor = builder.build();

		long now = System.currentTimeMillis();
		Event event = EventBuilder.withBody("00:00:00|test event",
				Charsets.UTF_8);
		event = interceptor.intercept(event);
		String timestampStr = event.getHeaders().get(Constants.TIMESTAMP);
		long newTimestamp = Long.parseLong(timestampStr);
		System.out.println(timestampStr + " - " + now + " = "
				+ (newTimestamp - now));
		assertThat(Long.parseLong(timestampStr) >= now, is(true));
	}

	@Test
	public void testGet() {
		Interceptor.Builder builder = new EventTimestampInterceptor.Builder();
		Context context = new Context();
		context.put(Constants.DELIMITER, "|");
		context.put(Constants.INDEX, "0");
		context.put(Constants.FORMAT, "[%d{yyyy-MM-dd HH:mm:ss}]");
		context.put(Constants.PRESERVE, "false");

		builder.configure(context);
		EventTimestampInterceptor interceptor = (EventTimestampInterceptor) builder
				.build();

		String data = "0|1||3||5";
		String res = interceptor.get(1, data.getBytes());
		assertThat(res, is("1"));

		res = interceptor.get(2, data.getBytes());
		assertThat(res, is(""));

		res = interceptor.get(5, data.getBytes());
		assertThat(res, is("5"));

		data = "0|1||3|| ";
		res = interceptor.get(5, data.getBytes());
		assertThat(res, is(" "));

		data = "0";
		res = interceptor.get(0, data.getBytes());
		assertThat(res, is("0"));
	}

	@Test
	public void testGetWithLongDelim() {
		Interceptor.Builder builder = new EventTimestampInterceptor.Builder();
		Context context = new Context();
		context.put(Constants.DELIMITER, "||");
		builder.configure(context);
		EventTimestampInterceptor interceptor = (EventTimestampInterceptor) builder
				.build();

		String data = "0||1||||3||||5";
		String res = interceptor.get(1, data.getBytes());
		assertThat(res, is("1"));

		res = interceptor.get(2, data.getBytes());
		assertThat(res, is(""));

		res = interceptor.get(5, data.getBytes());
		assertThat(res, is("5"));

		data = "0||1||||3|||| ";
		res = interceptor.get(5, data.getBytes());
		assertThat(res, is(" "));

		data = "0";
		res = interceptor.get(0, data.getBytes());
		assertThat(res, is("0"));
	}

	@Test
	public void testGetWithSpecialDelim() {
		Interceptor.Builder builder = new EventTimestampInterceptor.Builder();
		Context context = new Context();
		context.put(Constants.DELIMITER, "나해]");
		builder.configure(context);
		EventTimestampInterceptor interceptor = (EventTimestampInterceptor) builder
				.build();

		String data = "0나해]1나해]나해]3나해]나해]5";
		String res = interceptor.get(1, data.getBytes());
		assertThat(res, is("1"));

		res = interceptor.get(2, data.getBytes());
		assertThat(res, is(""));

		res = interceptor.get(5, data.getBytes());
		assertThat(res, is("5"));

		data = "0나해]1나해]나해]3나해]나해] ";
		res = interceptor.get(5, data.getBytes());
		assertThat(res, is(" "));

		data = "0";
		res = interceptor.get(0, data.getBytes());
		assertThat(res, is("0"));
	}

	@Test
	public void testGetWithBrokenLongDelim() {
		Interceptor.Builder builder = new EventTimestampInterceptor.Builder();
		Context context = new Context();
		context.put(Constants.DELIMITER, "||");
		builder.configure(context);
		EventTimestampInterceptor interceptor = (EventTimestampInterceptor) builder
				.build();

		String data = "0||1|||3||||5";
		String res = interceptor.get(1, data.getBytes());
		assertThat(res, is("1"));

		res = interceptor.get(2, data.getBytes());
		assertThat(res, is("|3"));

		res = interceptor.get(3, data.getBytes());
		assertThat(res, is(""));

		res = interceptor.get(4, data.getBytes());
		assertThat(res, is("5"));
	}
}
