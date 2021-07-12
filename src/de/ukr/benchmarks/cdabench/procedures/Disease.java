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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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

public class Disease extends CDAProcedure {

	private static final Logger LOG = Logger.getLogger(Delivery.class);

	public SQLStmt searchPatientsWithDiseaseSQL = new SQLStmt("select " + CDAConstants.TABLENAME_CDA + " from ("
			+ "select * " + "from " + CDAConstants.TABLENAME_CDA + " c "
			+ "where c.cda->'ClinicalDocument'->'recordTarget'->'patientRole'->'patient'->'administrativeGenderCode'->>'@code' = ? ) doc, "
			+ "json_array_elements(doc.cda->'ClinicalDocument'->'component'->'structuredBody'->'component') component, "
			+ "json_array_elements(component->'section'->'entry') entries "
			+ "WHERE component->'section'->'templateId'->>'@root' = ? "
			+ "AND component->'section'->'code'->>'@code' = ? "
			+ "AND component->'section'->'code'->>'@codeSystem' = ? " + "AND entries->>'@typeCode' = ? "
			+ "AND entries->'act'->>'@classCode' = ? " + "AND entries->'act'->>'@moodCode' = ? "
			+ "AND entries->'act'->'templateId'->>'@root' = ? "
			+ "AND entries->'act'->'entryRelationship'->>'@typeCode' = ? "
			+ "AND entries->'act'->'entryRelationship'->>'@inversionInd' = ? "
			+ "AND entries->'act'->'entryRelationship'->'observation'->>'@classCode' = ? "
			+ "AND entries->'act'->'entryRelationship'->'observation'->>'@moodCode' = ? "
			+ "AND entries->'act'->'entryRelationship'->'observation'->'templateId'->>'@root' = ? "
			+ "AND entries->'act'->'entryRelationship'->'observation'->'code'->>'@code' = ? "
			+ "AND entries->'act'->'entryRelationship'->'observation'->'code'->>'@codeSystem' = ? "
			+ "AND entries->'act'->'entryRelationship'->'observation'->'value'->>'@code' = ? "
			+ "AND entries->'act'->'entryRelationship'->'observation'->'value'->>'@codeSystem' = ?;");

	@Override
	public ResultSet run(Connection connection, HttpClient httpClient, MongoDatabase mongoDatabase,
			Collection existCollection, CDAWorker worker) throws SQLException {
		boolean trace = LOG.isDebugEnabled();
		String dbType = worker.getBenchmarkModule().getWorkloadConfiguration().getDBDriver();

		String[] gender = {"M", "F"};
		int seed = new Random().nextInt(gender.length);
		
		String administrativeGenderCode = gender[seed]; // 1
		String templateId = "2.16.840.1.113883.10.20.22.2.5.1"; // 2
		String sectionCode = "11450-4"; // 3
		String sectionCodeSystem = "2.16.840.1.113883.6.1"; // 4
		String entryTypeCode = "DRIV"; // 5
		String actClassCode = "ACT"; // 6
		String actMoodCode = "EVN"; // 7
		String actTemplateId = "2.16.840.1.113883.10.20.22.4.3"; // 8
		String entryRelationshipTypeCode = "SUBJ"; // 9
		String entryRelationshipInversionInd = "false"; // 10
		String observationClassCode = "OBS"; // 11
		String observationMoodCode = "EVN"; // 12
		String observationTemplateId = "2.16.840.1.113883.10.20.22.4.4"; // 13
		String observationCode = "64572001"; // 14
		String observationCodeSystem = "2.16.840.1.113883.6.96"; // 15
		String valueCode = "840539006"; // 16
		String valueCodeSystem = "2.16.840.1.113883.6.96"; // 17

		switch (dbType) {
		case CDAConfig.COUCHDB_DRIVER:
			// execute CouchDB Query
			try {
				String batchSize = "100";

				String query = "{\"selector\":{"
						+ "\"$and\":[{\"ClinicalDocument.recordTarget.patientRole.patient.administrativeGenderCode.@code\":\""
						+ administrativeGenderCode + "\"},"
						+ "{\"ClinicalDocument.component.structuredBody.component\":" + "{\"$elemMatch\":{"
						+ "\"section.templateId.@root\":\"" + templateId + "\"," + "\"section.code.@code\":\""
						+ sectionCode + "\"," + "\"section.code.@codeSystem\":\"" + sectionCodeSystem + "\","
						+ "\"section.entry\":{" + "\"$elemMatch\":{\"@typeCode\":\"" + entryTypeCode + "\","
						+ "\"act.@classCode\":\"" + actClassCode + "\"," + "\"act.@moodCode\":\"" + actMoodCode + "\","
						+ "\"act.templateId.@root\":\"" + actTemplateId + "\","
						+ "\"act.entryRelationship.@typeCode\":\"" + entryRelationshipTypeCode + "\","
						+ "\"act.entryRelationship.@inversionInd\":\"" + entryRelationshipInversionInd + "\","
						+ "\"act.entryRelationship.observation.@classCode\":\"" + observationClassCode + "\","
						+ "\"act.entryRelationship.observation.@moodCode\":\"" + observationMoodCode + "\","
						+ "\"act.entryRelationship.observation.templateId.@root\":\"" + observationTemplateId + "\","
						+ "\"act.entryRelationship.observation.code.@code\":\"" + observationCode + "\","
						+ "\"act.entryRelationship.observation.code.@codeSystem\":\"" + observationCodeSystem + "\","
						+ "\"act.entryRelationship.observation.value.@code\":\"" + valueCode + "\","
						+ "\"act.entryRelationship.observation.value.@codeSystem\":\"" + valueCodeSystem + "\"}}}}}]},"
						+ "\"limit\":" + batchSize + "}";

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
							terminalMessage.append("\n+--- SEARCH FEMALE PATIENTS WITH COVID DIAGNOSIS -----+\n");
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
						+ "			{\n"
						+ "				\"ClinicalDocument.recordTarget.patientRole.patient.administrativeGenderCode.@code\": \""+administrativeGenderCode+"\"\n"
						+ "			},\n"
						+ "			{\n"
						+ "				\"ClinicalDocument.component.structuredBody.component\": {\n"
						+ "					$elemMatch: {\n"
						+ "						\"section.templateId.@root\": \""+templateId+"\",\n"
						+ "						\"section.code.@code\": \""+sectionCode+"\",\n"
						+ "						\"section.code.@codeSystem\": \""+sectionCodeSystem+"\",\n"
						+ "						\"section.entry\": {\n"
						+ "							$elemMatch: {\n"
						+ "								\"@typeCode\": \""+entryTypeCode+"\",\n"
						+ "								\"act.@classCode\": \""+actClassCode+"\",\n"
						+ "								\"act.@moodCode\": \""+actMoodCode+"\",\n"
						+ "								\"act.templateId.@root\": \""+actTemplateId+"\",\n"
						+ "								\"act.entryRelationship.@typeCode\": \""+entryRelationshipTypeCode+"\",\n"
						+ "								\"act.entryRelationship.@inversionInd\": \""+entryRelationshipInversionInd+"\",\n"
						+ "								\"act.entryRelationship.observation.@classCode\": \""+observationClassCode+"\",\n"
						+ "								\"act.entryRelationship.observation.@moodCode\": \""+observationMoodCode+"\",\n"
						+ "								\"act.entryRelationship.observation.templateId.@root\": \""+observationTemplateId+"\",\n"
						+ "								\"act.entryRelationship.observation.code.@code\": \""+observationCode+"\",\n"
						+ "								\"act.entryRelationship.observation.code.@codeSystem\": \""+observationCodeSystem+"\",\n"
						+ "								\"act.entryRelationship.observation.value.@code\": \""+valueCode+"\",\n"
						+ "								\"act.entryRelationship.observation.value.@codeSystem\": \""+valueCodeSystem+"\"\n"
						+ "}}}}}]}");

				FindIterable<Document> results = collection.find(query);

				if (trace) {
					StringBuilder terminalMessage = new StringBuilder();
					terminalMessage.append("\n+--- SEARCH FEMALE PATIENTS WITH COVID DIAGNOSIS -----+\n");
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
			PreparedStatement searchPatientsWithDisease = this.getPreparedStatement(connection,
					searchPatientsWithDiseaseSQL);
			// prepare statement

			// 1st param -> administrativeGenderCode
			searchPatientsWithDisease.setString(1, administrativeGenderCode);
			// 2nd param -> templateId
			searchPatientsWithDisease.setString(2, templateId);
			// 3rd param -> sectionCode
			searchPatientsWithDisease.setString(3, sectionCode);
			// 4th param -> sectionCodeSystem
			searchPatientsWithDisease.setString(4, sectionCodeSystem);
			// 5th param -> entryTypeCode
			searchPatientsWithDisease.setString(5, entryTypeCode);
			// 6th param -> actClassCode
			searchPatientsWithDisease.setString(6, actClassCode);
			// 7th param -> actMoodCode
			searchPatientsWithDisease.setString(7, actMoodCode);
			// 8th param -> actTemplateId
			searchPatientsWithDisease.setString(8, actTemplateId);
			// 9th param -> entryRelationshipTypeCode
			searchPatientsWithDisease.setString(9, entryRelationshipTypeCode);
			// 10st param -> entryRelationshipInversionInd
			searchPatientsWithDisease.setString(10, entryRelationshipInversionInd);
			// 11th param -> observationClassCode
			searchPatientsWithDisease.setString(11, observationClassCode);
			// 12th param -> observationMoodCode
			searchPatientsWithDisease.setString(12, observationMoodCode);
			// 13th param -> observationTemplateId
			searchPatientsWithDisease.setString(13, observationTemplateId);
			// 14th param -> observationCode
			searchPatientsWithDisease.setString(14, observationCode);
			// 15th param -> observationCodeSystem
			searchPatientsWithDisease.setString(15, observationCodeSystem);
			// 16th param -> valueCode
			searchPatientsWithDisease.setString(16, valueCode);
			// 17th param -> valueCodeSystem
			searchPatientsWithDisease.setString(17, valueCodeSystem);

			ResultSet resultSet = null;

			// execute query
			if (trace) {
				LOG.trace("searchPatientsWithDisease START");
			}
			try {
				resultSet = searchPatientsWithDisease.getResultSet();
			} catch (SQLException e) {
				System.out.println("SQLException occurred during the execution of the query!");
				e.printStackTrace();
				if (trace) {
					LOG.trace("searchPatientsWithDisease END");
				}
				break;
			}

			if (trace) {
				StringBuilder terminalMessage = new StringBuilder();
				terminalMessage.append("\n+--- SEARCH FEMALE PATIENTS WITH COVID DIAGNOSIS -----+\n");
				terminalMessage.append(" Date: " + CDAUtil.getCurrentTime());
				terminalMessage.append("\n\n Disease: " + CDAConfig.diseaseCode);
				terminalMessage.append("\n\n Patients:\n");

				// print patients from results
				if (resultSet != null) {
					List<String> resultIds = new ArrayList<>();
					while (resultSet.next()) {
						resultIds.add(resultSet.getString(1));
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
