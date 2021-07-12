package de.ukr.benchmarks.cdabench.procedures;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.lt;
import static com.mongodb.client.model.Filters.gt;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.BsonDateTime;
import org.bson.Document;
import org.bson.conversions.Bson;
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

public class DiagnosisWomenAgeRange extends CDAProcedure {

	private static final Logger LOG = Logger.getLogger(Delivery.class);

	public SQLStmt searchDiagnosisWomenAgeRangeSQL = new SQLStmt("select component->>'section' " + "from "
			+ CDAConstants.TABLENAME_CDA
			+ " c, json_array_elements(c.cda#>'{ClinicalDocument,component,structuredBody,component}') component "
			+ "WHERE cda->'ClinicalDocument'->'recordTarget'->'patientRole'->'patient'->'administrativeGenderCode'->>'@code' = ? "
			+ "AND component->'section'->'templateId'->>'@root' = ? "
			+ "AND to_timestamp(cda->'ClinicalDocument'->'recordTarget'->'patientRole'->'patient'->'birthTime'->>'@value','YYYYMMDDHH24MISS') "
			+ "BETWEEN ?::timestamp AND ?::timestamp;");

	@Override
	public ResultSet run(Connection connection, HttpClient httpClient, MongoDatabase mongoDatabase,
			Collection existCollection, CDAWorker worker) throws SQLException {
		boolean trace = LOG.isDebugEnabled();
		String dbType = worker.getBenchmarkModule().getWorkloadConfiguration().getDBDriver();

		// Gender
		String administrativeGenderCode = "F";

		// calculate time frame
		DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

		Calendar cal = Calendar.getInstance(); // 20 years ago
		cal.add(Calendar.YEAR, -20);
		String minAge = format.format(Date.from(Instant.ofEpochMilli(cal.getTimeInMillis())));

		// Reset calendar
		cal = Calendar.getInstance();

		// 50 years ago
		cal.add(Calendar.YEAR, -50);
		String maxAge = format.format(Date.from(Instant.ofEpochMilli(cal.getTimeInMillis())));

		switch (dbType) {
		case CDAConfig.COUCHDB_DRIVER:
			// execute CouchDB Query
			try {
				String batchSize = "100";

				String query = "{\"selector\":{" + "\"$and\":["
						+ "{\"ClinicalDocument.recordTarget.patientRole.patient.administrativeGenderCode\":{"
						+ "\"@code\":\"" + administrativeGenderCode + "\"}},"
						+ "{\"ClinicalDocument.recordTarget.patientRole.patient.birthTime.@value\":{"
						+ "\"$and\":[{\"$gte\":\"" + maxAge + "\"},{\"$lt\":\"" + minAge + "\"}]}}]}," + "\"limit\":"
						+ batchSize + "}";
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
							terminalMessage
									.append("\n+-------- SEARCH DIAGNOSIS OF WOMEN BETWEEN AGE 20 TO 50 ---------+\n");
							terminalMessage.append(" Date: " + CDAUtil.getCurrentTime());
							terminalMessage.append("\n\n Disease: " + CDAConfig.diseaseCode);
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

				BasicDBObject query = BasicDBObject.parse("{$and: [\n"
						+ "	{\"ClinicalDocument.recordTarget.patientRole.patient.administrativeGenderCode.@code\": \""
						+ administrativeGenderCode + "\"}, \n"
						+ "	{\"ClinicalDocument.recordTarget.patientRole.patient.birthTime.@value\": { $gte: \""
						+ maxAge + "\", $lt: \"" + minAge + "\"}}]}");

				FindIterable<Document> results = collection.find(query);

				if (trace) {
					StringBuilder terminalMessage = new StringBuilder();
					terminalMessage.append("\n+-------- SEARCH DIAGNOSIS OF WOMEN BETWEEN AGE 20 TO 50 ---------+\n");
					terminalMessage.append(" Date: " + CDAUtil.getCurrentTime());
					terminalMessage.append("\n\n Disease: " + CDAConfig.diseaseCode);
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
			PreparedStatement searchDiagnosisWomenAgeRange = this.getPreparedStatement(connection,
					searchDiagnosisWomenAgeRangeSQL);

			// prepare statement
			// Section code
			String section = "2.16.840.1.113883.10.20.22.2.3.1";
			// calculate time frame
			DateFormat postgresFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			cal = Calendar.getInstance();

			// 20 years ago
			cal.add(Calendar.YEAR, -20);
			minAge = format.format(new Date().from(Instant.ofEpochMilli(cal.getTimeInMillis())));

			// Reset calendar
			cal = Calendar.getInstance();

			// 50 years ago
			cal.add(Calendar.YEAR, -50);
			maxAge = format.format(new Date().from(Instant.ofEpochMilli(cal.getTimeInMillis())));

			// 1st param -> gender
			searchDiagnosisWomenAgeRange.setString(1, administrativeGenderCode);
			// 2nd param -> section
			searchDiagnosisWomenAgeRange.setString(2, section);
			// 3rd param -> max (50y) (must be the smaller one!)
			searchDiagnosisWomenAgeRange.setString(3, maxAge);
			// 4th param -> min (20y)
			searchDiagnosisWomenAgeRange.setString(4, minAge);

			ResultSet resultSet = null;

			// execute query
			if (trace) {
				LOG.trace("searchDiagnosisWomenAgeRange START");
			}
			try {
				resultSet = searchDiagnosisWomenAgeRange.getResultSet();
			} catch (SQLException e) {
				System.out.println("SQLException occurred during the execution of the query!");
				e.printStackTrace();
				if (trace) {
					LOG.trace("searchDiagnosisWomenAgeRange END");
				}
				break;
			}

			if (trace) {
				StringBuilder terminalMessage = new StringBuilder();
				terminalMessage.append("\n+-------- SEARCH DIAGNOSIS OF WOMEN BETWEEN AGE 20 TO 50 ---------+\n");
				terminalMessage.append(" Date: " + CDAUtil.getCurrentTime());
				terminalMessage.append("\n\n Disease: " + CDAConfig.diseaseCode);
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
