package com.nilportugues.eventstore.postgres;

import com.github.javafaker.Faker;
import com.nilportugues.eventstore.Event;
import com.nilportugues.eventstore.EventFilter;
import com.nilportugues.eventstore.PostgresEventStore;
import org.junit.*;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class PostgresEventStoreTest {
	private static final long STREAM_VERSION = 1;
	private static final String EVENT_STREAM = "user_stream";
	private static final String MY_EVENT_ID = UUID.randomUUID().toString();
	private static final String MY_AGGREGATE_ID = UUID.randomUUID().toString();
	private static final String MY_AGGREGATE_NAME = "user";
	private static final String MY_EVENT_NAME = "user_registered";
	private static final String MY_EVENT_VERSION = "1.0.0";

	private PostgresEventStore eventStore;

	@Before
	public void setUp() throws Exception {
		final String jdbcUrl = "jdbc:postgresql://postgres:5432/postgres";
		final String tableName = "event_store";
		final String username = "postgres";
		final String password = "postgres";

		eventStore = new PostgresEventStore(new PostgresDataStore(jdbcUrl, tableName, username, password));
		eventStore.delete(EVENT_STREAM);

		final Event event = new Event();
		event.setAggregateId(MY_AGGREGATE_ID);
		event.setAggregateName(MY_AGGREGATE_NAME);
		event.setEventId(MY_EVENT_ID);
		event.setEventName(MY_EVENT_NAME);
		event.setEventVersion(MY_EVENT_VERSION);
		event.setPayload("{\"some_key\":\"some_value\"}");
		event.setMetadata("{\"metadata\":\"some_value\"}");
		event.setOccurredOn(ZonedDateTime.parse("2017-03-22T00:00:00Z"));

		final ArrayList<Event> eventStream = new ArrayList<>();
		eventStream.add(event);
		eventStore.appendTo(EVENT_STREAM, STREAM_VERSION, eventStream);
	}

	@After
	public void tearDown() {
		eventStore.delete(EVENT_STREAM);
	}

	@Test
	public void testAppendToStream() {
		Assert.assertTrue(eventStore.exists(EVENT_STREAM));
	}

	@Test
	public void testDeleteStream() {
		Assert.assertTrue(eventStore.exists(EVENT_STREAM));

		eventStore.delete(EVENT_STREAM);
		Assert.assertFalse(eventStore.exists(EVENT_STREAM));
	}

	@Test
	public void testIfStreamExists() {
		Assert.assertFalse(eventStore.exists("unexistent-stream"));
	}

	@Test
	public void testLoadEventStream() {

		Stream<Event> stream = eventStore.load(EVENT_STREAM, STREAM_VERSION);
		List<Event> list = stream.collect(Collectors.toList());
		Assert.assertTrue(list.size() == 1);
	}

	@Test
	public void testLoadEventFilter() {
		final EventFilter eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);

		final Stream<Event> stream = eventStore.load(eventFilter);
		Assert.assertNotNull(stream);

		final List<Event> list = stream.collect(Collectors.toList());
		Assert.assertTrue(list.size() == 1);

		final Event event = list.get(0);
		Assert.assertNotNull(event.getAggregateId());
		Assert.assertNotNull(event.getAggregateName());
		Assert.assertNotNull(event.getEventId());
		Assert.assertNotNull(event.getEventName());
		Assert.assertNotNull(event.getEventVersion());
		Assert.assertNotNull(event.getPayload());
		Assert.assertNotNull(event.getMetadata());
		Assert.assertNotNull(event.getOccurredOn());

	}

	@Test
	public void testLoadEventFilterStartTime() {
		EventFilter eventFilter;

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setTimeStart(ZonedDateTime.parse("2017-03-22T00:00:00Z"));
		Assert.assertTrue(eventStore.load(eventFilter).count() == 1);

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setTimeStart(ZonedDateTime.parse("2017-03-23T00:00:00Z"));
		Assert.assertTrue(eventStore.load(eventFilter).count() == 0);
	}

	@Test
	public void testLoadEventFilterEndTime() {
		EventFilter eventFilter;

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setTimeEnd(ZonedDateTime.parse("2017-03-22T00:00:00Z"));
		Assert.assertTrue(eventStore.load(eventFilter).count() == 1);

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setTimeEnd(ZonedDateTime.parse("2017-03-21T00:00:00Z"));
		Assert.assertTrue(eventStore.load(eventFilter).count() == 0);
	}

	@Test
	public void testLoadEventFilterAggregateId() {
		EventFilter eventFilter;

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setAggregateId(UUID.randomUUID().toString());
		Assert.assertTrue(eventStore.load(eventFilter).count() == 0);

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setAggregateId(MY_AGGREGATE_ID);
		Assert.assertTrue(eventStore.load(eventFilter).count() == 1);
	}

	@Test
	public void testLoadEventFilterAggregateName() {
		EventFilter eventFilter;

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setAggregateName("dogs");
		Assert.assertTrue(eventStore.load(eventFilter).count() == 0);

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setAggregateName(MY_AGGREGATE_NAME);
		Assert.assertTrue(eventStore.load(eventFilter).count() == 1);
	}

	@Test
	public void testLoadEventFilterEventId() {
		EventFilter eventFilter;

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setEventId(UUID.randomUUID().toString());
		Assert.assertTrue(eventStore.load(eventFilter).count() == 0);

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setEventId(MY_EVENT_ID);
		Assert.assertTrue(eventStore.load(eventFilter).count() == 1);
	}

	@Test
	public void testLoadEventFilterEventName() {
		EventFilter eventFilter;

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setEventName("dogs");
		Assert.assertTrue(eventStore.load(eventFilter).count() == 0);

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setEventName(MY_EVENT_NAME);
		Assert.assertTrue(eventStore.load(eventFilter).count() == 1);
	}

	@Test
	public void testLoadEventFilterEventVersion() {
		EventFilter eventFilter;

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setEventVersion("51.0.1");
		Assert.assertTrue(eventStore.load(eventFilter).count() == 0);

		eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setEventVersion(MY_EVENT_VERSION);
		Assert.assertTrue(eventStore.load(eventFilter).count() == 1);
	}

	@Test
	public void testPerformanceWithTwoMillionRecords() {

		final Faker faker = new Faker();
		int i = 0;

		while (i < 1000000) {

			final ArrayList<Event> eventStream = new ArrayList<>();

			for (int j = 0; j < 10000; j++) {
				final Event event = new Event();
				event.setAggregateId(UUID.randomUUID().toString());
				event.setAggregateName(MY_AGGREGATE_NAME);
				event.setEventId(UUID.randomUUID().toString());
				event.setEventName(MY_EVENT_NAME);
				event.setEventVersion(MY_EVENT_VERSION);

				event.setPayload("{\"pokemon\":\"" + faker.pokemon().name() + "\"}");
				event.setMetadata("{\"metadata\":\"" + faker.pokemon().location() + "\"}");
				event.setOccurredOn(ZonedDateTime.ofInstant(faker.date().birthday().toInstant(), ZoneId.of("UTC")));
				eventStream.add(event);
				i++;
			}

			System.out.println("Generating records: " + i + " of 1000000...");
			eventStore.appendTo(EVENT_STREAM, STREAM_VERSION, eventStream);
		}

		final EventFilter eventFilter = new EventFilter(EVENT_STREAM, STREAM_VERSION);
		eventFilter.setTimeStart(ZonedDateTime.parse("2017-03-22T00:00:00Z"));

		// Assert performance
		final ZonedDateTime start = ZonedDateTime.now();
		Assert.assertTrue(eventStore.load(eventFilter).count() > 0);
		final ZonedDateTime end = ZonedDateTime.now();
		long millis = Duration.between(start, end).toMillis();

		System.out.println("Querying into a 1 million rows data set took " + millis + " ms.");
	}

}
