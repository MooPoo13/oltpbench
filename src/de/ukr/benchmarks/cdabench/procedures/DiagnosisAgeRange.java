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
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Resource;
import org.xmldb.api.base.ResourceIterator;
import org.xmldb.api.base.ResourceSet;
import org.xmldb.api.modules.XPathQueryService;

import com.mongodb.BasicDBObject;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSFindIterable;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.oltpbenchmark.api.SQLStmt;
import com.oltpbenchmark.benchmarks.tpcc.procedures.Delivery;

import de.ukr.benchmarks.cdabench.CDAConfig;
import de.ukr.benchmarks.cdabench.CDAConstants;
import de.ukr.benchmarks.cdabench.CDAUtil;
import de.ukr.benchmarks.cdabench.CDAWorker;

public class DiagnosisAgeRange extends CDAProcedure {

	private static final Logger LOG = Logger.getLogger(Delivery.class);

	public SQLStmt searchDiagnosisWomenAgeRangeSQL = new SQLStmt("select component->>'section' " + "from "
			+ CDAConstants.TABLENAME_CDA
			+ " c, json_array_elements(c.cda#>'{ClinicalDocument,component,structuredBody,component}') component "
			+ "WHERE cda->'ClinicalDocument'->'recordTarget'->'patientRole'->'patient'->'administrativeGenderCode'->>'@code' = ? "
			+ "AND cda->'ClinicalDocument'->'recordTarget'->'patientRole'->'patient'->'administrativeGenderCode'->>'@codeSystem' = ? "
			+ "AND component->'section'->'templateId'->>'@root' = ? "
			+ "AND to_timestamp(cda->'ClinicalDocument'->'recordTarget'->'patientRole'->'patient'->'birthTime'->>'@value','YYYYMMDDHH24MISS') "
			+ "BETWEEN ?::timestamp AND ?::timestamp;");

	public static Instant between(Instant startInclusive, Instant endExclusive) {
		long startSeconds = startInclusive.getEpochSecond();
		long endSeconds = endExclusive.getEpochSecond();
		long random = ThreadLocalRandom.current().nextLong(startSeconds, endSeconds);

		return Instant.ofEpochSecond(random);
	}

	@Override
	public ResultSet run(final Connection connection, final HttpClient httpClient, final MongoDatabase mongoDatabase,
			final Collection existCollection, final CDAWorker worker) throws SQLException {
		boolean trace = LOG.isDebugEnabled();
		final String dbType = worker.getBenchmarkModule().getWorkloadConfiguration().getDBDriver();

		// Section templateId
		final String sectionTemplateId = "2.16.840.1.113883.10.20.22.2.3.1";

		final String[] gender = { "M", "F" };
		int seed = new Random().nextInt(gender.length);

		// Gender
		final String administrativeGenderCode = gender[seed];

		final String administrativeGenderCodeSystem = "2.16.840.1.113883.5.1";

		// calculate time frame
		final DateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

		final Instant hundredYearsAgo = Instant.now().minus(Duration.ofDays(100 * 365));

		final Instant now = Instant.now();

		final Instant maxDate = between(hundredYearsAgo, now);

		String maxAge = format.format(Date.from(maxDate));

		final Instant minDate = between(maxDate, now);

		String minAge = format.format(Date.from(minDate));

		switch (dbType) {
		case CDAConfig.COUCHDB_DRIVER:
			// execute CouchDB Query
			try {
				String batchSize = "100";

				String query = "{\"selector\":{" + "\"$and\":["
						+ "{\"ClinicalDocument.recordTarget.patientRole.patient.administrativeGenderCode\":{"
						+ "\"@code\":\"" + administrativeGenderCode + "\"," + "\"@codeSystem\":\""
						+ administrativeGenderCodeSystem + "\"}},"
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
			try {
				String existQuery = "//administrativeGenderCode[@code='" + administrativeGenderCode + "' "
						+ "and @codeSystem='" + administrativeGenderCodeSystem + "']/parent::node()"
						+ "/birthTime[@value > '" + maxAge + "' and @value < '" + minAge + "']"
						+ "/ancestor::node()/component/structuredBody/component/section" + "/templateId[@root='"
						+ sectionTemplateId + "']/parent::node()";

				XPathQueryService xpqs = (XPathQueryService) existCollection.getService("XPathQueryService", "1.0");
				xpqs.setProperty("indent", "yes");
				xpqs.setNamespace(null, "urn:hl7-org:v3");

				ResourceSet result = xpqs.query(existQuery);

				if (trace) {
					StringBuilder terminalMessage = new StringBuilder();
					terminalMessage.append("\n+-------- SEARCH DIAGNOSIS OF WOMEN BETWEEN AGE 20 TO 50 ---------+\n");
					terminalMessage.append(" Date: " + CDAUtil.getCurrentTime());
					terminalMessage.append("\n\n Disease: " + CDAConfig.diseaseCode);
					terminalMessage.append("\n\n Patients:\n");

					// print patients from results

					ResourceIterator i = result.getIterator();
					Resource res = null;

					if (i.hasMoreResources()) {
						while (i.hasMoreResources()) {
							res = i.nextResource();
							terminalMessage.append("\n\n " + res.getId() + "\n");
						}

					} else {
						terminalMessage.append("\n\n Error while iterating ResultSet!\n");
					}
					terminalMessage.append("+-----------------------------------------------------------------+\n\n");
					LOG.trace(terminalMessage.toString());
				}

			} catch (Exception e) {
				System.out.println("Failed to query data from eXistDB for CDABench: ");
				e.printStackTrace();
			}

			break;
		case CDAConfig.MONGODB_DRIVER:
			// execute MongoDB Query
			try {
				// MongoCollection<Document> collection =
				// mongoDatabase.getCollection(CDAConstants.TABLENAME_CDA);

				GridFSBucket bucket = GridFSBuckets.create(mongoDatabase, CDAConstants.TABLENAME_CDA);

				BasicDBObject query = BasicDBObject.parse("{$and: [\n"
						+ "	{\"ClinicalDocument.recordTarget.patientRole.patient.administrativeGenderCode.@code\": \""
						+ administrativeGenderCode + "\"}, \n"
						+ "	{\"ClinicalDocument.recordTarget.patientRole.patient.administrativeGenderCode.@codeSystem\": \""
						+ administrativeGenderCodeSystem + "\"}, \n"
						+ "	{\"ClinicalDocument.recordTarget.patientRole.patient.birthTime.@value\": { $gte: \""
						+ maxAge + "\", $lt: \"" + minAge + "\"}}]}");

				// FindIterable<Document> results = collection.find(query);

				GridFSFindIterable results = bucket.find(query);

				if (trace) {
					StringBuilder terminalMessage = new StringBuilder();
					terminalMessage.append("\n+-------- SEARCH DIAGNOSIS OF WOMEN BETWEEN AGE 20 TO 50 ---------+\n");
					terminalMessage.append(" Date: " + CDAUtil.getCurrentTime());
					terminalMessage.append("\n\n Disease: " + CDAConfig.diseaseCode);
					terminalMessage.append("\n\n Patients:\n");

					// print patients from results
					if (results != null) {
						/*
						 * for (Document document : results) { terminalMessage.append("\n\n " +
						 * document.get("_id").toString() + "\n"); }
						 */

						for (GridFSFile document : results) {
							terminalMessage.append("\n\n " + document.getId() + "\n");
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
			PreparedStatement searchDiagnosisAgeRange = this.getPreparedStatement(connection,
					searchDiagnosisWomenAgeRangeSQL);

			// prepare statement

			// calculate time frame
			final DateFormat postgresFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

			maxAge = postgresFormat.format(Date.from(maxDate));

			minAge = postgresFormat.format(Date.from(minDate));

			// 1st param -> gender code
			searchDiagnosisAgeRange.setString(1, administrativeGenderCode);
			// 2nd param -> gender code system
			searchDiagnosisAgeRange.setString(2, administrativeGenderCodeSystem);
			// 3rd param -> section templateId
			searchDiagnosisAgeRange.setString(3, sectionTemplateId);
			// 4th param -> max age (must be the smaller one!)
			searchDiagnosisAgeRange.setString(4, maxAge);
			// 5th param -> min age
			searchDiagnosisAgeRange.setString(5, minAge);

			ResultSet resultSet = null;

			// execute query
			if (trace) {
				LOG.trace("searchDiagnosisWomenAgeRange START");
			}
			try {
				resultSet = searchDiagnosisAgeRange.getResultSet();
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
