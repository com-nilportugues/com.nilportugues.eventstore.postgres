package com.nilportugues.eventstore;

import com.nilportugues.eventstore.postgres.PostgresDataStore;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class PostgresEventStore implements EventStore {

	private final PostgresDataStore dataStore;

	public PostgresEventStore(PostgresDataStore dataStore) {
		this.dataStore = dataStore;
	}

	@Override
	public boolean exists(String streamName) {
		try {
			return dataStore.exists(streamName);
		} catch (SQLException ignored) {
			ignored.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean exists(String streamName, Long streamVersion) {
		try {
			return dataStore.existsByVersion(streamName, streamVersion);
		} catch (SQLException ignored) {
			ignored.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean exists(String streamName, Long streamVersion, String eventName) {
		try {
			return dataStore.existsWithEventName(streamName, streamVersion, eventName);
		} catch (SQLException ignored) {
			ignored.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean exists(String streamName, Long streamVersion, String eventName, String eventVersion) {
		try {
			return dataStore.existsByEvent(streamName, streamVersion, eventName, eventVersion);
		} catch (SQLException ignored) {
			ignored.printStackTrace();
			return false;
		}
	}

	@Override
	public Stream<Event> load(String streamName, Long streamVersion) {
		try {
			return dataStore.findByFilter(new EventFilter(streamName, streamVersion));
		} catch (SQLException ignored) {
			ignored.printStackTrace();
			return (new ArrayList<Event>()).stream();
		}
	}

	@Override
	public Stream<Event> load(EventFilter eventFilter) {
		try {
			return dataStore.findByFilter(eventFilter);
		} catch (SQLException ignored) {
			ignored.printStackTrace();
			return (new ArrayList<Event>()).stream();
		}
	}

	@Override
	public void appendTo(String streamName, Long version, List<Event> list) {
		try {
			dataStore.storeStream(streamName, version, list);
		} catch (SQLException ignored) {
			ignored.printStackTrace();
		}
	}

	@Override
	public void appendTo(String streamName, Long version, Event event) {
		try {
			final ArrayList<Event> list = new ArrayList<>();
			list.add(event);
			dataStore.storeStream(streamName, version, list);
		} catch (SQLException ignored) {
			ignored.printStackTrace();
		}
	}

	@Override
	public void delete(String streamName) {
		try {
			dataStore.deleteStream(streamName);
		} catch (SQLException ignored) {
			ignored.printStackTrace();
		}
	}

	@Override
	public void delete(String streamName, Long version) {
		try {
			dataStore.deleteStreamByVersion(streamName, version);
		} catch (SQLException ignored) {
			ignored.printStackTrace();
		}
	}
}
