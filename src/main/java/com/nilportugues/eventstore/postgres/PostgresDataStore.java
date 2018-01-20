package com.nilportugues.eventstore.postgres;

import com.healthmarketscience.sqlbuilder.*;
import com.healthmarketscience.sqlbuilder.custom.postgresql.PgLimitClause;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbColumn;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSchema;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbSpec;
import com.healthmarketscience.sqlbuilder.dbspec.basic.DbTable;
import com.nilportugues.eventstore.Event;
import com.nilportugues.eventstore.EventFilter;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.RowProcessor;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

public class PostgresDataStore {
	private static final Logger LOG = LoggerFactory.getLogger(PostgresDataStore.class);

	private static final String TABLE_NAME = "event_store";
	private static final String COLUMN_ID = "event_store_id";
	private static final String COLUMN_STREAM_NAME = "stream_name";
	private static final String COLUMN_STREAM_VERSION = "stream_version";
	private static final String COLUMN_EVENT_ID = "event_id";
	private static final String COLUMN_EVENT_NAME = "event_name";
	private static final String COLUMN_EVENT_VERSION = "event_version";
	private static final String COLUMN_AGGREGATE_ID = "aggregate_id";
	private static final String COLUMN_AGGREGATE_NAME = "aggregate_name";
	private static final String COLUMN_PAYLOAD = "payload";
	private static final String COLUMN_METADATA = "metadata";
	private static final String COLUMN_OCCURRED_ON = "occurred_on";
	private static final String COLUMN_OCCURRED_ON_YEAR = "occurred_on_year";
	private static final String COLUMN_OCCURRED_ON_MONTH = "occurred_on_month";
	private static final String COLUMN_OCCURRED_ON_DAY = "occurred_on_day";
	private static final String COLUMN_OCCURRED_ON_HOUR = "occurred_on_hour";
	private static final String COLUMN_OCCURRED_ON_MINUTE = "occurred_on_minute";

	private final Connection connection;
	private final PGSimpleDataSource dataSource;

	private DbTable table;
	private DbSpec spec;
	private DbSchema schema;
	private DbColumn idColumn;
	private DbColumn streamNameColumn;
	private DbColumn streamVersionColumn;
	private DbColumn eventIdColumn;
	private DbColumn eventNameColumn;
	private DbColumn eventVersionColumn;
	private DbColumn aggregateIdColumn;
	private DbColumn aggregateNameColumn;
	private DbColumn payloadColumn;
	private DbColumn occurredOnColumn;
	private DbColumn metadataColumn;
	private DbColumn occurredOnYearColumn;
	private DbColumn occurredOnMonthColumn;
	private DbColumn occurredOnMinuteColumn;
	private DbColumn occurredOnHourColumn;
	private DbColumn occurredOnDayColumn;

	public PostgresDataStore(final String url, final String tableName, final String username, final String password)
			throws Exception {

		dataSource = new PGSimpleDataSource();
		dataSource.setUrl(url);
		dataSource.setPassword(username);
		dataSource.setUser(password);
		connection = dataSource.getConnection();

		spec = new DbSpec();
		schema = spec.addDefaultSchema();
		table = schema.addTable(tableName);

		prepareQueryBuilder(tableName);
		pgDefineTableSchema();
	}

	private void prepareQueryBuilder(String tableName) {
		spec = new DbSpec();
		schema = spec.addDefaultSchema();
		table = schema.addTable(tableName);
	}

	private void pgDefineTableSchema() throws SQLException {

		idColumn = table.addColumn(COLUMN_ID, "SERIAL", null);
		streamNameColumn = table.addColumn(COLUMN_STREAM_NAME, Types.VARCHAR, 255);
		streamVersionColumn = table.addColumn(COLUMN_STREAM_VERSION, "SMALLINT", null);
		eventIdColumn = table.addColumn(COLUMN_EVENT_ID, Types.VARCHAR, 255);
		eventNameColumn = table.addColumn(COLUMN_EVENT_NAME, Types.VARCHAR, 255);
		eventVersionColumn = table.addColumn(COLUMN_EVENT_VERSION, Types.VARCHAR, 50);
		aggregateNameColumn = table.addColumn(COLUMN_AGGREGATE_NAME, Types.VARCHAR, 255);
		aggregateIdColumn = table.addColumn(COLUMN_AGGREGATE_ID, Types.VARCHAR, 255);
		payloadColumn = table.addColumn(COLUMN_PAYLOAD, "JSONB", null);
		metadataColumn = table.addColumn(COLUMN_METADATA, "JSONB", null);
		occurredOnColumn = table.addColumn(COLUMN_OCCURRED_ON, "timestamptz", null);

		// Optimization for speed
		occurredOnYearColumn = table.addColumn(COLUMN_OCCURRED_ON_YEAR, "SMALLINT", null);
		occurredOnMonthColumn = table.addColumn(COLUMN_OCCURRED_ON_MONTH, "SMALLINT", null);
		occurredOnDayColumn = table.addColumn(COLUMN_OCCURRED_ON_DAY, "SMALLINT", null);
		occurredOnHourColumn = table.addColumn(COLUMN_OCCURRED_ON_HOUR, "SMALLINT", null);
		occurredOnMinuteColumn = table.addColumn(COLUMN_OCCURRED_ON_MINUTE, "SMALLINT", null);

		if (hasEventStoreTable()) {
			return;
		}

		createTableAndIndexes();
	}

	private boolean hasEventStoreTable() throws SQLException {

		final String checkIfTableExistsSQL = "SELECT EXISTS (\n" + "   SELECT 1 \n"
				+ "   FROM   pg_catalog.pg_class c\n"
				+ "   JOIN   pg_catalog.pg_namespace n ON n.oid = c.relnamespace\n" + "   WHERE  n.nspname = 'public'\n"
				+ "   AND    c.relname = '" + TABLE_NAME + "'\n" + "   AND    c.relkind = 'r'    -- only tables\n"
				+ "   );";

		final PreparedStatement stmt = connection.prepareStatement(checkIfTableExistsSQL);
		final ResultSet resultSet = stmt.executeQuery();

		if (resultSet.next() && resultSet.getBoolean(1)) {
			LOG.debug("Tried creating table name {} ... but already exists.", table.getTableNameSQL());
			return true;
		}

		return false;
	}

	private void createTableAndIndexes() {
		try {
			final CreateTableQuery createTable = new CreateTableQuery(table, true);
			createTable.addColumnConstraint(idColumn, ConstraintClause.primaryKey(COLUMN_ID));
			createTable.addColumnConstraint(eventIdColumn, ConstraintClause.unique(COLUMN_EVENT_ID));
			createTable.addColumnConstraint(aggregateIdColumn, ConstraintClause.unique(COLUMN_AGGREGATE_ID));

			final String createTableSQL = createTable.validate().toString();
			final Statement stmt = connection.createStatement();
			stmt.execute(createTableSQL);

			final ArrayList<String> indexQueries = new ArrayList<>();
			indexQueries.add(new CreateIndexQuery(table, COLUMN_STREAM_NAME).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_STREAM_VERSION).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_EVENT_NAME).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_EVENT_ID).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_EVENT_VERSION).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_AGGREGATE_NAME).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_AGGREGATE_ID).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_PAYLOAD).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_METADATA).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_OCCURRED_ON).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_OCCURRED_ON_YEAR).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_OCCURRED_ON_MONTH).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_OCCURRED_ON_DAY).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_OCCURRED_ON_HOUR).toString());
			indexQueries.add(new CreateIndexQuery(table, COLUMN_OCCURRED_ON_MINUTE).toString());

			connection.createStatement().execute(String.join(";", indexQueries));
		} catch (SQLException exception) {
			LOG.debug("Tried creating table name {} ... but failed.", table.getTableNameSQL());
		}
	}

	public boolean exists(String streamName) throws SQLException {
		return existsByEvent(streamName, null, null, null);
	}

	public boolean existsByVersion(String streamName, Long streamVersion) throws SQLException {
		return existsByEvent(streamName, streamVersion, null, null);
	}

	public boolean existsByEvent(String streamName, Long streamVersion, String eventName, String eventVersion)
			throws SQLException {
		// Build
		final SelectQuery query = new SelectQuery().addAllTableColumns(table);
		final ComboCondition where = query.getWhereClause();

		// Filter
		where.addCondition(BinaryCondition.equalTo(streamNameColumn, streamName));
		Optional.ofNullable(streamVersion)
				.ifPresent(value -> where.addCondition(BinaryCondition.equalTo(streamVersionColumn, value)));
		Optional.ofNullable(eventName)
				.ifPresent(value -> where.addCondition(BinaryCondition.equalTo(eventNameColumn, value)));
		Optional.ofNullable(eventVersion)
				.ifPresent(value -> where.addCondition(BinaryCondition.equalTo(eventVersionColumn, value)));

		query.addCustomization(new PgLimitClause("1"));

		// Query
		final String existsQuery = query.validate().toString();

		final Statement statement = connection.createStatement();
		final ResultSet resultSet = statement.executeQuery(existsQuery);

		return resultSet.next() && (resultSet.getInt(1) > 0);
	}

	public boolean existsWithEventName(String streamName, Long streamVersion, String eventName) throws SQLException {
		return existsByEvent(streamName, streamVersion, eventName, null);
	}

	public Stream<Event> findByFilter(EventFilter eventFilter) throws SQLException {

		// Build
		final SelectQuery query = new SelectQuery().addColumns(streamNameColumn, streamVersionColumn, eventIdColumn,
				eventNameColumn, eventVersionColumn, aggregateIdColumn, aggregateNameColumn, payloadColumn,
				metadataColumn, occurredOnColumn);
		final ComboCondition where = query.getWhereClause();

		// Optimized DateTime Filtering
		optimizeDateTimeFilter(eventFilter, where);

		// Regular Filtering
		eventFilter.getStreamName()
				.ifPresent(value -> where.addCondition(BinaryCondition.equalTo(streamNameColumn, value)));
		eventFilter.getStreamVersion()
				.ifPresent(value -> where.addCondition(BinaryCondition.equalTo(streamVersionColumn, value)));
		eventFilter.getAggregateId()
				.ifPresent(value -> where.addCondition(BinaryCondition.equalTo(aggregateIdColumn, value)));
		eventFilter.getAggregateName()
				.ifPresent(value -> where.addCondition(BinaryCondition.equalTo(aggregateNameColumn, value)));
		eventFilter.getAggregateId()
				.ifPresent(value -> where.addCondition(BinaryCondition.equalTo(aggregateIdColumn, value)));
		eventFilter.getEventName()
				.ifPresent(value -> where.addCondition(BinaryCondition.equalTo(eventNameColumn, value)));
		eventFilter.getEventId().ifPresent(value -> where.addCondition(BinaryCondition.equalTo(eventIdColumn, value)));
		eventFilter.getEventVersion()
				.ifPresent(value -> where.addCondition(BinaryCondition.equalTo(eventVersionColumn, value)));
		eventFilter.getTimeStart()
				.ifPresent(value -> where.addCondition(BinaryCondition.greaterThanOrEq(occurredOnColumn, value)));
		eventFilter.getTimeEnd()
				.ifPresent(value -> where.addCondition(BinaryCondition.lessThanOrEq(occurredOnColumn, value)));

		// Query & Hydrate
		final String selectQuery = query.validate().toString() + " ORDER BY " + COLUMN_OCCURRED_ON + " ASC";
		LOG.debug(selectQuery);

		final QueryRunner runner = new QueryRunner(dataSource);

		return runner.query(selectQuery, new EventBeanListHandler(Event.class)).stream();
	}

	private void optimizeDateTimeFilter(final EventFilter eventFilter, final ComboCondition where) {

		// Time Range
		if (eventFilter.getTimeStart().isPresent() && eventFilter.getTimeEnd().isPresent()) {
			final ZonedDateTime start = eventFilter.getTimeStart().get().withZoneSameInstant(ZoneId.of("UTC"));
			final ZonedDateTime end = eventFilter.getTimeEnd().get().withZoneSameInstant(ZoneId.of("UTC"));

			if (optimizeDateTimeFilterByYear(eventFilter, where, start, end)) {
				return;
			}

			eventFilter.getTimeStart().ifPresent(value -> where
					.addCondition(BinaryCondition.greaterThanOrEq(occurredOnYearColumn, value.getYear())));
			eventFilter.getTimeEnd().ifPresent(
					value -> where.addCondition(BinaryCondition.lessThanOrEq(occurredOnYearColumn, value.getYear())));
			return;
		}

		// Single Date
		if (eventFilter.getTimeStart().isPresent()) {
			final ZonedDateTime start = eventFilter.getTimeStart().get().withZoneSameInstant(ZoneId.of("UTC"));
			where.addCondition(BinaryCondition.greaterThanOrEq(occurredOnYearColumn, start.getYear()));
			where.addCondition(BinaryCondition.greaterThanOrEq(occurredOnMonthColumn, start.getMonthValue()));
			where.addCondition(BinaryCondition.greaterThanOrEq(occurredOnDayColumn, start.getDayOfMonth()));
			where.addCondition(BinaryCondition.greaterThanOrEq(occurredOnHourColumn, start.getHour()));
			where.addCondition(BinaryCondition.greaterThanOrEq(occurredOnMinuteColumn, start.getMinute()));
			return;
		}

		// Single Date
		if (eventFilter.getTimeEnd().isPresent()) {
			final ZonedDateTime end = eventFilter.getTimeEnd().get().withZoneSameInstant(ZoneId.of("UTC"));
			where.addCondition(BinaryCondition.lessThanOrEq(occurredOnYearColumn, end.getYear()));
			where.addCondition(BinaryCondition.lessThanOrEq(occurredOnMonthColumn, end.getMonthValue()));
			where.addCondition(BinaryCondition.lessThanOrEq(occurredOnDayColumn, end.getDayOfMonth()));
			where.addCondition(BinaryCondition.lessThanOrEq(occurredOnHourColumn, end.getHour()));
			where.addCondition(BinaryCondition.lessThanOrEq(occurredOnMinuteColumn, end.getMinute()));
		}
	}

	private boolean optimizeDateTimeFilterByYear(final EventFilter eventFilter, final ComboCondition where,
			final ZonedDateTime start, final ZonedDateTime end) {
		if (start.getYear() == end.getYear()) {
			where.addCondition(BinaryCondition.equalTo(occurredOnYearColumn, start.getYear()));

			if (optimizeDateTimeFilterByMonth(eventFilter, where, start, end)) {
				return true;
			}

			eventFilter.getTimeStart().ifPresent(value -> where
					.addCondition(BinaryCondition.greaterThanOrEq(occurredOnMonthColumn, value.getMonthValue())));
			eventFilter.getTimeEnd().ifPresent(value -> where
					.addCondition(BinaryCondition.lessThanOrEq(occurredOnMonthColumn, value.getMonthValue())));
			return true;
		}
		return false;
	}

	private boolean optimizeDateTimeFilterByMonth(final EventFilter eventFilter, final ComboCondition where,
			final ZonedDateTime start, final ZonedDateTime end) {

		if (start.getMonthValue() == end.getMonthValue()) {
			where.addCondition(BinaryCondition.equalTo(occurredOnMonthColumn, start.getMonthValue()));

			if (optimizeDateTimeFilterByDay(eventFilter, where, start, end)) {
				return true;
			}

			eventFilter.getTimeStart().ifPresent(value -> where
					.addCondition(BinaryCondition.greaterThanOrEq(occurredOnMonthColumn, value.getMonthValue())));
			eventFilter.getTimeEnd().ifPresent(value -> where
					.addCondition(BinaryCondition.lessThanOrEq(occurredOnMonthColumn, value.getMonthValue())));
			return true;
		}
		return false;
	}

	private boolean optimizeDateTimeFilterByDay(final EventFilter eventFilter, final ComboCondition where,
			final ZonedDateTime start, final ZonedDateTime end) {
		if (start.getDayOfMonth() == end.getDayOfMonth()) {
			where.addCondition(BinaryCondition.equalTo(occurredOnDayColumn, start.getDayOfMonth()));

			if (optimizeDateTimeFilterByHour(eventFilter, where, start, end)) {
				return true;
			}

			eventFilter.getTimeStart().ifPresent(value -> where
					.addCondition(BinaryCondition.greaterThanOrEq(occurredOnDayColumn, value.getDayOfMonth())));
			eventFilter.getTimeEnd().ifPresent(value -> where
					.addCondition(BinaryCondition.lessThanOrEq(occurredOnDayColumn, value.getDayOfMonth())));
			return true;
		}
		return false;
	}

	private boolean optimizeDateTimeFilterByHour(final EventFilter eventFilter, final ComboCondition where,
			final ZonedDateTime start, final ZonedDateTime end) {
		if (start.getHour() == end.getHour()) {
			where.addCondition(BinaryCondition.equalTo(occurredOnHourColumn, start.getHour()));
			eventFilter.getTimeStart().ifPresent(value -> where
					.addCondition(BinaryCondition.greaterThanOrEq(occurredOnMinuteColumn, value.getMinute())));
			eventFilter.getTimeEnd().ifPresent(value -> where
					.addCondition(BinaryCondition.lessThanOrEq(occurredOnMinuteColumn, value.getMinute())));
			return true;
		}
		return false;
	}

	public void storeStream(String streamName, Long version, List<Event> list) throws SQLException {
		try {
			final String query = new InsertQuery(table).addPreparedColumns(streamNameColumn, streamVersionColumn,
					eventIdColumn, eventNameColumn, eventVersionColumn, aggregateIdColumn, aggregateNameColumn,
					payloadColumn, metadataColumn, occurredOnColumn, occurredOnYearColumn, occurredOnMonthColumn,
					occurredOnDayColumn, occurredOnHourColumn, occurredOnMinuteColumn).validate().toString()
					+ " ON CONFLICT DO NOTHING"; // Don't fail if inserted twice.

			final String insertQuery = String.join(";", Collections.nCopies(list.size(), query));

			// Query
			startTransaction();
			final PreparedStatement statement = connection.prepareStatement(insertQuery);

			int i = 1;
			for (final Event event : list) {
				statement.setString(i, streamName);
				i++;

				statement.setLong(i, version);
				i++;

				statement.setString(i, event.getEventId());
				i++;

				statement.setString(i, event.getEventName());
				i++;

				statement.setString(i, event.getEventVersion());
				i++;

				statement.setString(i, event.getAggregateId());
				i++;

				statement.setString(i, event.getAggregateName());
				i++;

				// PAYLOAD JSONB field
				final PGobject jsonPayload = new PGobject();
				jsonPayload.setType("json");
				jsonPayload.setValue(event.getPayload());
				statement.setObject(i, jsonPayload);
				i++;

				// METADATA JSONB field
				final PGobject jsonMetadata = new PGobject();
				jsonMetadata.setType("json");
				jsonMetadata.setValue(event.getMetadata());
				statement.setObject(i, jsonMetadata);
				i++;

				// Occurred on: TimeStamp
				statement.setTimestamp(i, Timestamp.from(event.getOccurredOn().toInstant()));
				i++;

				final ZonedDateTime utc = event.getOccurredOn().withZoneSameInstant(ZoneId.of("UTC"));

				// Occurred on: Year
				statement.setInt(i, utc.getYear());
				i++;

				// Occurred on: Month
				statement.setInt(i, utc.getMonth().getValue());
				i++;

				// Occurred on: Day
				statement.setInt(i, utc.getDayOfMonth());
				i++;

				// Occurred on: Hour
				statement.setInt(i, utc.getHour());
				i++;

				// Occurred on: Minute
				statement.setInt(i, utc.getMinute());
				i++;
			}

			statement.execute();
			endTransaction();
		} catch (SQLException e) {
			rollbackTransaction();
			LOG.error(e.getMessage());
		}
	}

	public void deleteStream(String streamName) throws SQLException {
		deleteStreamByVersion(streamName, null);
	}

	public void deleteStreamByVersion(String streamName, Long version) throws SQLException {
		try {
			// Build
			final DeleteQuery query = new DeleteQuery(table);
			final ComboCondition where = query.getWhereClause();

			where.addCondition(BinaryCondition.equalTo(streamNameColumn, streamName));
			Optional.ofNullable(version)
					.ifPresent(value -> where.addCondition(BinaryCondition.equalTo(streamVersionColumn, value)));

			// Query
			startTransaction();
			final String deleteQuery = query.validate().toString();
			connection.createStatement().execute(deleteQuery);
			endTransaction();
		} catch (SQLException e) {
			rollbackTransaction();
			LOG.error(e.getMessage());
		}
	}

	private void startTransaction() throws SQLException {
		connection.setAutoCommit(false);
	}

	private void endTransaction() throws SQLException {
		connection.commit();
	}

	private void rollbackTransaction() throws SQLException {
		connection.rollback();
	}

	private static class EventBeanListHandler extends BeanListHandler<Event> {

		public EventBeanListHandler(Class<? extends Event> type) {
			super(type);
		}

		public EventBeanListHandler(Class<? extends Event> type, RowProcessor convert) {
			super(type, convert);
		}

		@Override
		@SuppressWarnings("unchecked")
		public List<Event> handle(ResultSet rs) throws SQLException {

			try {
				List newlist = new LinkedList();
				while (rs.next()) {
					newlist.add(toBean(rs));
				}
				return newlist;
			} catch (SQLException ex) {
				throw new RuntimeException(ex);
			}
		}

		private Object toBean(ResultSet rs) throws SQLException {

			final Event event = new Event();
			event.setEventId(rs.getString(COLUMN_EVENT_ID));
			event.setEventName(rs.getString(COLUMN_EVENT_NAME));
			event.setEventVersion(rs.getString(COLUMN_EVENT_VERSION));
			event.setPayload(rs.getString(COLUMN_PAYLOAD));
			event.setAggregateId(rs.getString(COLUMN_AGGREGATE_ID));
			event.setAggregateName(rs.getString(COLUMN_AGGREGATE_NAME));
			event.setMetadata(rs.getString(COLUMN_METADATA));

			final String formattedDate = rs.getString(COLUMN_OCCURRED_ON).replace(" ", "T") + ":00";
			event.setOccurredOn(ZonedDateTime.parse(formattedDate, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
					.withZoneSameInstant(ZoneId.of("UTC")));

			return event;
		}
	}

}