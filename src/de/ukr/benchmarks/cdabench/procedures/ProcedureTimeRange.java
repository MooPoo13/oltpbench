package de.ukr.benchmarks.cdabench.procedures;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse.BodyHandlers;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.xmldb.api.base.Collection;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery;

import de.ukr.benchmarks.cdabench.CDAConfig;
import de.ukr.benchmarks.cdabench.CDAConstants;
import de.ukr.benchmarks.cdabench.CDAUtil;
import de.ukr.benchmarks.cdabench.CDAWorker;

public class ProcedureTimeRange extends CDAProcedure {

	private static final Logger LOG = Logger.getLogger(Delivery.class);

	public SQLStmt searchProcedureTimeRangeSQL = new SQLStmt("select count(*) "
			+ "from (select component->'section' section " + "from cda c, "
			+ "json_array_elements(c.cda->'ClinicalDocument'->'component'->'structuredBody'->'component') component"
			+ "where component->'section'->'templateId'->>'@root' = ? "
			+ "AND component->'section'->'code'->>'@code' = ?) section, "
			+ "json_array_elements(section.section->'entry') entries "
			+ "where entries->'procedure'->>'@classCode'  = ? " + "AND entries->'procedure'->'code'->>'@code'  = ? "
			+ "AND entries->'procedure'->'code'->>'@codeSystem'  = ? "
			+ "AND entries->'procedure'->'code'->>'@displayName'  = ? "
			+ "AND to_timestamp(entries->'procedure'->'effectiveTime'->>'@value','YYYYMMDDHH24MISS') "
			+ "BETWEEN ?::timestamp AND ?::timestamp;");

	public static Instant between(Instant startInclusive, Instant endExclusive) {
		long startSeconds = startInclusive.getEpochSecond();
		long endSeconds = endExclusive.getEpochSecond();
		long random = ThreadLocalRandom.current().nextLong(startSeconds, endSeconds);

		return Instant.ofEpochSecond(random);
	}

	@Override
	public ResultSet run(Connection connection, HttpClient httpClient, MongoDatabase mongoDatabase,
			Collection existCollection, CDAWorker worker) throws SQLException {
		boolean trace = LOG.isDebugEnabled();
		String dbType = worker.getBenchmarkModule().getWorkloadConfiguration().getDBDriver();

		// Section templateId
		String templateId = "2.16.840.1.113883.10.20.22.2.7.1";

		// Section section code
		String sectionCode = "47519-4";

		String sectionCodeSystem = "2.16.840.1.113883.6.1";

		// Procedure class code
		String procClassCode = "PROC";

		// Procedure code
		String procCode = "18027006";

		// Procedure code system -> SNOMED
		String procCodeSystem = "2.16.840.1.113883.6.96";

		// calculate time frame
		final DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

		final Instant hundredYearsAgo = Instant.now().minus(Duration.ofDays(100 * 365));

		final Instant now = Instant.now();

		final Instant minDateInstant = between(hundredYearsAgo, now);

		// min date
		// String minDate = "20200101000000";
		String minDate = format.format(Date.from(minDateInstant));

		final Instant maxDateInstant = between(minDateInstant, now);

		// max date
		// String maxDate = "20210101000000";
		String maxDate = format.format(Date.from(maxDateInstant));

		switch (dbType) {
		case CDAConfig.COUCHDB_DRIVER:
			// execute CouchDB Query
			try {
				String batchSize = "100";

				String query = "{\"selector\":{" + "\"ClinicalDocument.component.structuredBody.component\":{"
						+ "\"$elemMatch\":{" + "\"section.templateId.@root\":\"" + templateId + "\","
						+ "\"section.code.@code\":\"" + sectionCode + "\"," + "\"section.code.@codeSystem\":\""
						+ sectionCodeSystem + "\"," + "\"section.entry\":{" + "\"$elemMatch\":{"
						+ "\"procedure.@classCode\":\"" + procClassCode + "\"," + "\"procedure.code.@code\":\""
						+ procCode + "\"," + "\"procedure.code.@codeSystem\":\"" + procCodeSystem + "\","
						+ "\"procedure.effectiveTime.@value\":{" + "\"$and\":[{" + "\"$gte\":\"" + minDate + "\"},"
						+ "{\"$lt\":\"" + maxDate + "\"}]}}}}}}," + "\"limit\":" + batchSize + "}";

				// configure http request
				HttpRequest request = HttpRequest.newBuilder()
						.uri(URI.create(worker.getWorkloadConfiguration().getDBConnection() + "/_find"))
						.timeout(Duration.ofMinutes(2)).header("Content-Type", "application/json")
						.POST(BodyPublishers.ofString(query)).build();

				// send request
				HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());

				// get http response
				int status = response.statusCode();

				if (status > 299) {
					System.out.println("Failed to query data from Couch for CDABench " + response.body());

				} else {
					// System.out.println("Successfully query data from Couch for CDABench " +
					// response.body());
					try {
						JSONObject body = new JSONObject(response.body());

						JSONArray results = body.getJSONArray("docs");

						if (trace) {
							StringBuilder terminalMessage = new StringBuilder();
							terminalMessage.append("\n+-------- COUNT PROCEDURES IN TIME RANGE ---------+\n");
							terminalMessage.append(" Date: " + CDAUtil.getCurrentTime());
							terminalMessage.append("\n\n Disease: " + procCode);
							terminalMessage.append("\n\n Patients:\n");

							// print patients from results
							if (results != null) {
								for (int i = 0; i < results.length(); i++) {
									JSONObject document = results.getJSONObject(i);
									terminalMessage.append("\n\n " + document.getString("_id") + "\n");

								}
							} else {
								terminalMessage.append("\n\n Error while iterating ResultSet!\n");
							}
							terminalMessage
									.append("+-----------------------------------------------------------------+\n\n");
							LOG.trace(terminalMessage.toString());
						}

					} catch (Exception e) {
						System.out.println("Failed response from Couch for CDABench ");
						e.printStackTrace();
					}
				}

			} catch (Exception e) {
				System.out.println("Failed to query data from CouchDB for CDABench: ");
				e.printStackTrace();
			}
			break;
		case CDAConfig.EXISTDB_DRIVER:
			// execute ExistDB Query
			break;
		case CDAConfig.MONGODB_DRIVER:
			// execute MongoDB Query
			try {
				MongoCollection<Document> collection = mongoDatabase.getCollection(CDAConstants.TABLENAME_CDA);

				BasicDBObject query = BasicDBObject
						.parse("{\"ClinicalDocument.component.structuredBody.component\": {\n"
								+ "			$elemMatch: {\n" + "				\"section.templateId.@root\": \""
								+ templateId + "\",\n" + "				\"section.code.@code\": \"" + sectionCode
								+ "\",\n" + "				\"section.code.@codeSystem\": \"" + sectionCodeSystem
								+ "\",\n" + "				\"section.entry\": {\n" + "					$elemMatch: {\n"
								+ "						\"procedure.@classCode\": \"" + procClassCode + "\",\n"
								+ "						\"procedure.code.@code\": \"" + procCode + "\",\n"
								+ "						\"procedure.code.@codeSystem\": \"" + procCodeSystem + "\",\n"
								+ "						\"procedure.effectiveTime.@value\": { $gte: \"" + minDate
								+ "\", $lt: \"" + maxDate + "\"}\n" + "					}\n" + "				}\n"
								+ "			}}\n" + "}");

				FindIterable<Document> results = collection.find(query);

				if (trace) {
					StringBuilder terminalMessage = new StringBuilder();
					terminalMessage.append("\n+-------- COUNT PROCEDURES IN TIME RANGE ---------+\n");
					terminalMessage.append(" Date: " + CDAUtil.getCurrentTime());
					terminalMessage.append("\n\n Disease: " + procCode);
					terminalMessage.append("\n\n Patients:\n");

					// print patients from results
					if (results != null) {
						for (Document document : results) {
							terminalMessage.append("\n\n " + document.get("_id").toString() + "\n");
						}
					} else {
						terminalMessage.append("\n\n Error while iterating ResultSet!\n");
					}
					terminalMessage.append("+-----------------------------------------------------------------+\n\n");
					LOG.trace(terminalMessage.toString());
				}
			} catch (Exception e) {
				System.out.println("Failed to query data from MongoDB for CDABench: ");
				e.printStackTrace();
			}
			break;
		case CDAConfig.POSTGRESQL_DRIVER:
			// execute PostgreSQL Query
			PreparedStatement countProcedureTimeRange = this.getPreparedStatement(connection,
					searchProcedureTimeRangeSQL);

			// prepare statement

			// calculate time frame
			DateFormat postgresFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			// min date
			minDate = postgresFormat.format(Date.from(minDateInstant));

			// max date
			maxDate = postgresFormat.format(Date.from(maxDateInstant));

			// 1st param -> templateId
			countProcedureTimeRange.setString(1, templateId);
			// 2nd param -> sectionCode
			countProcedureTimeRange.setString(2, sectionCode);
			// 3rd param -> procClassCode
			countProcedureTimeRange.setString(3, procClassCode);
			// 4th param -> procCode
			countProcedureTimeRange.setString(4, procCode);
			// 1st param -> procCodeSystem
			countProcedureTimeRange.setString(5, procCodeSystem);
			// 3rd param -> minDate
			countProcedureTimeRange.setString(6, minDate);
			// 4th param -> maxDate
			countProcedureTimeRange.setString(7, maxDate);

			ResultSet resultSet = null;

			// execute query
			if (trace) {
				LOG.trace("countProcedureTimeRange START");
			}
			try {
				resultSet = countProcedureTimeRange.getResultSet();
			} catch (SQLException e) {
				System.out.println("SQLException occurred during the execution of the query!");
				e.printStackTrace();
				if (trace) {
					LOG.trace("countProcedureTimeRange END");
				}
				break;
			}

			if (trace) {
				StringBuilder terminalMessage = new StringBuilder();
				terminalMessage.append("\n+-------- COUNT PROCEDURES IN TIME RANGE ---------+\n");
				terminalMessage.append(" Date: " + CDAUtil.getCurrentTime());
				terminalMessage.append("\n\n Disease: " + procCode);
				terminalMessage.append("\n\n Patients:\n");

				// print patients from results
				if (resultSet != null) {
					while (resultSet.next()) {
						terminalMessage.append("\n\n Doc-Id: " + resultSet.getString(1) + "\n");
					}
				} else {
					terminalMessage.append("\n\n Error while iterating ResultSet!\n");
				}

				/*
				 * for (int i = 1; i <= nrOfPatien; i++) { if (orderIDs[i - 1] >= 0) {
				 * terminalMessage.append("  Patient "); terminalMessage.append(i < 10 ? " " :
				 * ""); terminalMessage.append(i); terminalMessage.append(": Order number ");
				 * terminalMessage.append(orderIDs[i - 1]);
				 * terminalMessage.append(" was delivered.\n"); } } // FOR
				 */
				terminalMessage.append("+-----------------------------------------------------------------+\n\n");
				LOG.trace(terminalMessage.toString());
			}
			return resultSet;
		}

		return null;

	}
}
