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

public class Disease extends CDAProcedure {

	private static final Logger LOG = Logger.getLogger(Delivery.class);

	public SQLStmt searchPatientsWithDiseaseSQL = new SQLStmt("select " + CDAConstants.TABLENAME_CDA + " from ("
			+ "select * " + "from " + CDAConstants.TABLENAME_CDA + " c "
			+ "where c.cda->'ClinicalDocument'->'recordTarget'->'patientRole'->'patient'->'administrativeGenderCode'->>'@code' = ? "
			+ "and  c.cda->'ClinicalDocument'->'recordTarget'->'patientRole'->'patient'->'administrativeGenderCode'->>'@codeSystem' = ?) doc, "
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

		String[] gender = { "M", "F" };
		int seed = new Random().nextInt(gender.length);
		
		String administrativeGenderCode = gender[seed]; // 1
		String administrativeGenderCodeSystem = "2.16.840.1.113883.5.1"; // 2
		String templateId = "2.16.840.1.113883.10.20.22.2.5.1"; // 3
		String sectionCode = "11450-4"; // 4
		String sectionCodeSystem = "2.16.840.1.113883.6.1"; // 5
		String entryTypeCode = "DRIV"; // 6
		String actClassCode = "ACT"; // 7
		String actMoodCode = "EVN"; // 8
		String actTemplateId = "2.16.840.1.113883.10.20.22.4.3"; // 9
		String entryRelationshipTypeCode = "SUBJ"; // 10
		String entryRelationshipInversionInd = "false"; // 11
		String observationClassCode = "OBS"; // 12
		String observationMoodCode = "EVN"; // 13
		String observationTemplateId = "2.16.840.1.113883.10.20.22.4.4"; // 14
		String observationCode = "64572001"; // 15
		String observationCodeSystem = "2.16.840.1.113883.6.96"; // 16
		String valueCode = "840539006"; // 17
		String valueCodeSystem = "2.16.840.1.113883.6.96"; // 18
		
		switch (dbType) {
		case CDAConfig.COUCHDB_DRIVER:
			// execute CouchDB Query
			try {
				String batchSize = "100";

				String query = "{\"selector\":{"
						+ "\"$and\":[{\"ClinicalDocument.recordTarget.patientRole.patient.administrativeGenderCode.@code\":\""
						+ administrativeGenderCode + "\"},"
						+ "{\"ClinicalDocument.recordTarget.patientRole.patient.administrativeGenderCode.@codeSystem\":\""
								+ administrativeGenderCodeSystem + "\"},"
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
			try {
				String existQuery = "//administrativeGenderCode[@code='" + administrativeGenderCode + "' " + "and @codeSystem='"
						+ administrativeGenderCodeSystem + "']/ancestor::node()"
						+ "/component/structuredBody/component/section" + "/templateId[@root='" + templateId
						+ "']/parent::node()" + "/code[@code='" + sectionCode + "' and " + "@codeSystem='"
						+ sectionCodeSystem + "']" + "/parent::node()/entry[@typeCode='" + entryTypeCode + "']"
						+ "/act[@classCode='" + actClassCode + "' and " + "@moodCode='" + actMoodCode + "']"
						+ "/templateId[@root='" + actTemplateId + "']" + "/parent::node()/entryRelationship[@typeCode='"
						+ entryRelationshipTypeCode + "' and " + "@inversionInd='" + entryRelationshipInversionInd + "']"
						+ "/observation[@classCode='" + observationClassCode + "' and @moodCode='" + observationMoodCode
						+ "']/templateId[@root='" + observationTemplateId + "']/parent::node()" + "/code[@code='"
						+ observationCode + "' and " + "@codeSystem='" + observationCodeSystem + "']"
						+ "/parent::node()/value[@code='" + valueCode + "' and @codeSystem='" + valueCodeSystem
						+ "']/ancestor::ClinicalDocument";

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
				//	MongoCollection<Document> collection = mongoDatabase.getCollection(CDAConstants.TABLENAME_CDA);
				
				GridFSBucket bucket = GridFSBuckets.create(mongoDatabase, CDAConstants.TABLENAME_CDA);
				
				BasicDBObject query = BasicDBObject.parse("{$and: [\n"
						+ "			{\n"
						+ "				\"ClinicalDocument.recordTarget.patientRole.patient.administrativeGenderCode.@code\": \""+administrativeGenderCode+"\"\n"
						+ "			},\n"
						+ "			{\n"
						+ "				\"ClinicalDocument.recordTarget.patientRole.patient.administrativeGenderCode.@codeSystem\": \""+administrativeGenderCodeSystem+"\"\n"
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

				// FindIterable<Document> results = collection.find(query);
				
				GridFSFindIterable results = bucket.find(query);

				if (trace) {
					StringBuilder terminalMessage = new StringBuilder();
					terminalMessage.append("\n+--- SEARCH FEMALE PATIENTS WITH COVID DIAGNOSIS -----+\n");
					terminalMessage.append(" Date: " + CDAUtil.getCurrentTime());
					terminalMessage.append("\n\n Disease: " + CDAConfig.diseaseCode);
					terminalMessage.append("\n\n Patients:\n");

					// print patients from results
					if (results != null) {
						/*for (Document document : results) {
							terminalMessage.append("\n\n " + document.get("_id").toString() + "\n");
						}*/
						
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
			PreparedStatement searchPatientsWithDisease = this.getPreparedStatement(connection,
					searchPatientsWithDiseaseSQL);
			// prepare statement

			// 1st param -> administrativeGenderCode
			searchPatientsWithDisease.setString(1, administrativeGenderCode);
			// 2nd param -> administrativeGenderCodeSystem
			searchPatientsWithDisease.setString(2, administrativeGenderCodeSystem);
			// 3rd param -> templateId
			searchPatientsWithDisease.setString(3, templateId);
			// 4th param -> sectionCode
			searchPatientsWithDisease.setString(4, sectionCode);
			// 5th param -> sectionCodeSystem
			searchPatientsWithDisease.setString(5, sectionCodeSystem);
			// 6th param -> entryTypeCode
			searchPatientsWithDisease.setString(6, entryTypeCode);
			// 7th param -> actClassCode
			searchPatientsWithDisease.setString(7, actClassCode);
			// 8th param -> actMoodCode
			searchPatientsWithDisease.setString(8, actMoodCode);
			// 9th param -> actTemplateId
			searchPatientsWithDisease.setString(9, actTemplateId);
			// 10th param -> entryRelationshipTypeCode
			searchPatientsWithDisease.setString(10, entryRelationshipTypeCode);
			// 11st param -> entryRelationshipInversionInd
			searchPatientsWithDisease.setString(11, entryRelationshipInversionInd);
			// 12th param -> observationClassCode
			searchPatientsWithDisease.setString(12, observationClassCode);
			// 13th param -> observationMoodCode
			searchPatientsWithDisease.setString(13, observationMoodCode);
			// 14th param -> observationTemplateId
			searchPatientsWithDisease.setString(14, observationTemplateId);
			// 15th param -> observationCode
			searchPatientsWithDisease.setString(15, observationCode);
			// 16th param -> observationCodeSystem
			searchPatientsWithDisease.setString(16, observationCodeSystem);
			// 17th param -> valueCode
			searchPatientsWithDisease.setString(17, valueCode);
			// 18th param -> valueCodeSystem
			searchPatientsWithDisease.setString(18, valueCodeSystem);

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
