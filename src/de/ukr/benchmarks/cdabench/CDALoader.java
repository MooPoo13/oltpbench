package de.ukr.benchmarks.cdabench;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpRequest.BodyPublishers;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandlers;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.postgresql.util.PGobject;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.XMLDBException;
import org.xmldb.api.modules.XMLResource;
import org.xmldb.api.modules.XQueryService;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.model.GridFSUploadOptions;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import com.oltpbenchmark.api.Loader;

public class CDALoader extends Loader<CDABenchmark> {
	private static final Logger LOG = Logger.getLogger(CDALoader.class);

	/**
	 * Custom implementation of the original LoaderThread class to support REST
	 * connections to CouchDB.
	 * 
	 * @author juliatitze
	 *
	 */
	public abstract class CouchDBLoaderThread extends LoaderThread {
		private final HttpClient httpClient;

		public CouchDBLoaderThread() throws SQLException, IOException {
			this.httpClient = CDALoader.this.benchmark.makeCouchDBConnection();
		}

		@Override
		public void run() {
			try {
				this.load(this.httpClient);
			} catch (Exception ex) {
				String msg = String.format("Unexpected error when loading %s database",
						CDALoader.this.benchmark.getBenchmarkName().toUpperCase());
				LOG.error(msg, ex);
				throw new RuntimeException(ex);
			}
		}

		/**
		 * This is the method that each CDALoaderThread has to implement.
		 * 
		 * @param dbConnector
		 */
		public abstract void load(HttpClient httpClient);

		public HttpClient getCouchDbConnector() {
			return this.httpClient;
		}

	}

	/**
	 * Custom implementation of the original LoaderThread class to support
	 * connections to a MongoDB collection via MongoCollection&lt;Document&gt;.
	 * 
	 * @author juliatitze
	 *
	 */
	public abstract class MongoDBLoaderThread extends LoaderThread {
		private final MongoDatabase database;

		public MongoDBLoaderThread() throws SQLException {
			this.database = CDALoader.this.benchmark.makeMongoDBConnection();
		}

		@Override
		public void run() {
			try {
				this.load(this.database);
			} catch (Exception ex) {
				String msg = String.format("Unexpected error when loading %s database",
						CDALoader.this.benchmark.getBenchmarkName().toUpperCase());
				LOG.error(msg, ex);
				throw new RuntimeException(ex);
			}
		}

		/**
		 * This is the method that each CDALoaderThread has to implement.
		 * 
		 * @param mongoClient
		 */
		public abstract void load(MongoDatabase database);

		public MongoDatabase getDatabase() {
			return this.database;
		}
	}

	/**
	 * Custom implementation of the original LoaderThread class to support
	 * connections to ExistDB via XML:DB API/Collection.
	 * 
	 * @author juliatitze
	 *
	 */
	public abstract class ExistDBLoaderThread extends LoaderThread {
		private final Collection collection;

		public ExistDBLoaderThread() throws SQLException, ClassNotFoundException, InstantiationException,
				IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
				SecurityException, XMLDBException, IOException {
			this.collection = CDALoader.this.benchmark.makeExistDBConnection();
		}

		@Override
		public void run() {
			try {
				this.load(collection);
			} catch (Exception ex) {
				String msg = String.format("Unexpected error when loading %s database",
						CDALoader.this.benchmark.getBenchmarkName().toUpperCase());
				LOG.error(msg, ex);
				throw new RuntimeException(ex);
			}
		}

		/**
		 * This is the method that each CDALoaderThread has to implement.
		 * 
		 * @param mongoClient
		 */
		public abstract void load(Collection collection);

		public Collection getCollection() {
			return this.collection;
		}
	}

	public CDALoader(CDABenchmark benchmark) {
		super(benchmark);
	}

	@Override
	public List<LoaderThread> createLoaderThreads() throws SQLException {
		LOG.info("Creating loader threads...");
		LOG.info("DataDir: " + this.workConf.getDataDir());

		List<LoaderThread> threads = new ArrayList<LoaderThread>();

		if (LOG.isDebugEnabled())
			LOG.debug("Creating loader threads...");

		// load data into database depending on the chosen database
		if (this.workConf.getDBDriver().equals(CDAConfig.COUCHDB_DRIVER)) {
			// CouchDB
			try {
				CouchDBLoaderThread t = new CouchDBLoaderThread() {
					@Override
					public void load(HttpClient httpClient) {
						if (LOG.isDebugEnabled())
							LOG.debug("Starting to load Documents ");

						loadDocumentsCouchDB(httpClient);
					}

					@Override
					public void load(Connection conn) throws SQLException {
						// do nothing
					}
				};
				threads.add(t);
			} catch (IOException | SQLException e) {
				LOG.debug(e.getMessage());
				// transRollback(conn);
			}
		} else if (this.workConf.getDBDriver().equals(CDAConfig.EXISTDB_DRIVER)) {
			// ExistDB
			try {
				ExistDBLoaderThread t = new ExistDBLoaderThread() {

					@Override
					public void load(Collection collection) {
						if (LOG.isDebugEnabled())
							LOG.debug("Starting to load Documents ");

						loadDocumentsExistDB(collection);
					}

					@Override
					public void load(Connection conn) throws SQLException {
						// do nothing
					}

				};
				threads.add(t);
			} catch (Exception e) {
				LOG.debug(e.getMessage());
			}

		} else if (this.workConf.getDBDriver().equals(CDAConfig.MONGODB_DRIVER)) {
			// MongoDB
			try {
				MongoDBLoaderThread t = new MongoDBLoaderThread() {

					@Override
					public void load(MongoDatabase database) {
						if (LOG.isDebugEnabled())
							LOG.debug("Starting to load Documents ");
						loadDocumentsMongoDB(database);
					}

					@Override
					public void load(Connection conn) throws SQLException {
						// do nothing
					}

				};
				threads.add(t);
			} catch (Exception e) {
				LOG.debug(e.getMessage());
				LOG.info(e.getMessage());
				e.printStackTrace();
			}
		} else if (this.workConf.getDBDriver().equals(CDAConfig.POSTGRESQL_DRIVER)) {
			LoaderThread t = new LoaderThread() {
				@Override
				public void load(Connection connection) throws SQLException {
					if (LOG.isDebugEnabled())
						LOG.debug("Starting to load Documents ");

					// WAREHOUSE
					loadDocumentsPostgres(connection);
				}
			};
			threads.add(t);
		} else {
			// we got a problem over here
			if (LOG.isDebugEnabled())
				LOG.debug("Houston, we got a problem. Db-Type " + this.workConf.getDBDriver()
						+ " did not match any implemented db-types.");
		}

		return threads;
	}

	private List<File> collectFiles(Path directory) {
		// collect files from data directory
		List<File> files = null;
		try {
			files = Files.walk(directory).filter(Files::isRegularFile).map(p -> p.toFile())
					.collect(Collectors.toList());
		} catch (IOException e) {
			LOG.error("Failed to load data from data directory for CDABench" + e.getMessage());
			if (LOG.isDebugEnabled())
				LOG.debug(e.getStackTrace().toString());
		}

		return files;
	}

	public String getFileContentAsString(Path path) {
		String fileContent = null;
		try {
			fileContent = Files.readString(path);
		} catch (IOException e) {
			LOG.error("Failed to read content from file " + path.toString() + " CDABench" + e.getMessage());
			if (LOG.isDebugEnabled())
				LOG.debug(e.getStackTrace().toString());
		}
		return fileContent;
	}

	@SuppressWarnings("unchecked")
	public Map<String, Object> getFileContentAsMap(File file) {
		Map<String, Object> fileContent = null;
		try {
			ObjectMapper mapper = new ObjectMapper();
			fileContent = mapper.readValue(file, Map.class);
		} catch (IOException e) {
			LOG.error("Failed to read content from file " + file.getName() + " for CDABench" + e.getMessage());
			if (LOG.isDebugEnabled())
				LOG.debug(e.getStackTrace().toString());
		}
		return fileContent;
	}

	public int loadDocumentsCouchDB(HttpClient httpClient) {
		List<File> files = this.collectFiles(Paths.get(workConf.getDataDir()));

		if (files != null) {
			for (File file : files) {
				// parse doc to string
				String jsonContent = this.getFileContentAsString(file.toPath());

				if (jsonContent != null) {
					try {
						// configure http request
						HttpRequest request = HttpRequest.newBuilder().uri(URI.create(this.workConf.getDBConnection()))
								.timeout(Duration.ofMinutes(2)).header("Content-Type", "application/json")
								.POST(BodyPublishers.ofFile(file.toPath())).build();

						// send request
						HttpResponse<String> response = httpClient.send(request, BodyHandlers.ofString());

						// get http response
						int status = response.statusCode();

						if (status > 299) {
							LOG.error("Failed to load data for CDABench " + response.body());

						} else {
							if (LOG.isDebugEnabled())
								LOG.debug("Successfully loaded data for CDABench " + response.body());
						}
					} catch (IOException | InterruptedException e) {
						LOG.error("Failed to load data to CouchDB for CDABench" + e.getMessage());
						if (LOG.isDebugEnabled())
							LOG.debug(e.getStackTrace().toString());
					}
				}
			}
			return 0;
		}
		return 1;

	}

	public int loadDocumentsExistDB(Collection collection) {
		List<File> files = this.collectFiles(Paths.get(workConf.getDataDir()));

		if (files != null) {
			for (File file : files) {
				// parse doc to string
				String xmlContent = this.getFileContentAsString(file.toPath());

				if (xmlContent != null) {
					// get Collection
					try {
						// create new XMLResource; an id will be assigned to the new resource
						XMLResource resource = (XMLResource) collection.createResource(null, "XMLResource");

						resource.setContent(file);
						System.out.print("storing document " + resource.getId() + "...");
						collection.storeResource(resource);
						System.out.println("ok.");

					} catch (XMLDBException e) {
						LOG.error("Failed to load data to ExistDB for CDABench" + e.getMessage());
						if (LOG.isDebugEnabled())
							LOG.debug(e.getStackTrace().toString());
					}

				}
			}
			return 0;
		}
		return 1;
	}

	public int loadDocumentsMongoDB(MongoDatabase database) {
		String collectionName = "cda";
		/*
		 * MongoCollection<Document> collection = null;
		 * 
		 * // drop collection if exists if(collectionExists(database, collectionName)) {
		 * collection = database.getCollection(collectionName); collection.drop(); }
		 * 
		 * database.createCollection(collectionName); collection =
		 * database.getCollection(collectionName);
		 */

		GridFSBucket bucket = GridFSBuckets.create(database, collectionName);

		List<File> files = this.collectFiles(Paths.get(workConf.getDataDir()));

		if (files != null) {
			for (File file : files) {
				/*
				 * // parse doc to string String jsonContent =
				 * this.getFileContentAsString(file.toPath());
				 * 
				 * if (jsonContent != null) { try { Document doc = Document.parse(jsonContent);
				 * 
				 * collection.insertOne(doc);
				 * 
				 * } catch (Exception e) {
				 * LOG.error("Failed to load data to MongoDB for CDABench " + e.getMessage());
				 * if (LOG.isDebugEnabled()) LOG.debug(e.getStackTrace().toString()); }
				 * 
				 * }
				 */
				try {
					InputStream streamToUploadFrom = new FileInputStream(file);

					// Create some custom options
					GridFSUploadOptions options = new GridFSUploadOptions().chunkSizeBytes(358400);

					ObjectId fileId = bucket.uploadFromStream(file.getName(), streamToUploadFrom, options);
					
					LOG.info("Uploaded file: " + fileId); 

				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}

			}
			return 0;
		}
		return 1;
	}

	public static boolean collectionExists(MongoDatabase database, final String collectionName) {
		MongoIterable<String> collectionNames = database.listCollectionNames();
		for (final String name : collectionNames) {
			if (name.equalsIgnoreCase(collectionName)) {
				return true;
			}
		}
		return false;
	}

	public int loadDocumentsPostgres(Connection connection) {
		boolean fail = false;

		List<File> files = this.collectFiles(Paths.get(workConf.getDataDir()));

		if (files != null) {
			LOG.info("Starting to load files to Postgres...");
			for (File file : files) {
				// parse doc to string
				String jsonContent = this.getFileContentAsString(file.toPath());

				if (jsonContent != null) {
					try {

						// prepare statement
						PreparedStatement insPrepStmt = getInsertStatement(connection, CDAConstants.TABLENAME_CDA);

						PGobject jsonObject = new PGobject();
						jsonObject.setType("json");
						jsonObject.setValue(jsonContent);

						insPrepStmt.setObject(1, UUID.randomUUID());
						insPrepStmt.setObject(2, jsonObject);

						insPrepStmt.execute();

						transCommit(connection);
					} catch (SQLException e) {
						LOG.error("Failed to load data from file " + file.getPath() + " Postgres for CDABench "
								+ e.getMessage());
						if (LOG.isDebugEnabled())
							LOG.debug(e.getStackTrace().toString());
						fail = true;
					} finally {
						if (fail) {
							LOG.debug("Rolling back changes from last batch");
							transRollback(connection);
						}

					}
				}
			}
			return 0;
		}
		return 1;
	}

	/**
	 * Copied from TPCC benchmark
	 * 
	 * @param connection
	 */
	protected void transRollback(Connection connection) {
		try {
			connection.rollback();
		} catch (SQLException se) {
			LOG.debug(se.getMessage());
		}
	}

	/**
	 * Copied from TPCC benchmark
	 * 
	 * @param connection
	 */
	protected void transCommit(Connection connection) {
		try {
			connection.commit();
		} catch (SQLException se) {
			LOG.debug(se.getMessage());
			transRollback(connection);
		}
	}

	/**
	 * Get insert statement for postgres.
	 * 
	 * @param conn
	 */
	private PreparedStatement getInsertStatement(Connection conn, String tableName) throws SQLException {
		String sql = "INSERT INTO cda(id, cda) VALUES (?, ?);";
		PreparedStatement stmt = conn.prepareStatement(sql);
		return stmt;
	}
}
