package de.ukr.benchmarks.cdabench;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.http.HttpClient;
import java.net.http.HttpClient.Redirect;
import java.net.http.HttpClient.Version;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.xmldb.api.DatabaseManager;
import org.xmldb.api.base.Collection;
import org.xmldb.api.base.Database;
import org.xmldb.api.base.XMLDBException;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.oltpbenchmark.WorkloadConfiguration;
import com.oltpbenchmark.api.BenchmarkModule;
import com.oltpbenchmark.api.Loader;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.catalog.Catalog;

import de.ukr.benchmarks.cdabench.procedures.DiagnosisWomenAgeRange;

public class CDABenchmark extends BenchmarkModule {
	private static final Logger LOG = Logger.getLogger(CDABenchmark.class);

	public CDABenchmark(WorkloadConfiguration workConf) {
		super("cdabench", workConf, false);
	}

	@Override
	protected List<Worker<? extends BenchmarkModule>> makeWorkersImpl(boolean verbose) throws IOException {
		List<Worker<? extends BenchmarkModule>> workers = new ArrayList<Worker<? extends BenchmarkModule>>();
		for (int i = 0; i < workConf.getTerminals(); ++i) {
			workers.add(new CDAWorker(this, i));
		} // FOR
		return workers;
	}

	@Override
	protected Loader<? extends BenchmarkModule> makeLoaderImpl() throws SQLException {
		return new CDALoader(this);
	}

	@Override
	protected Package getProcedurePackageImpl() {
		return (DiagnosisWomenAgeRange.class.getPackage());
	}

	// --------------------------------------------------------------------------
	// DATABASE CONNETION
	// --------------------------------------------------------------------------

	/**
	 *
	 * @return
	 * @throws SQLException
	 */
	@Override
	public Connection makeConnection() throws SQLException {
		if (this.workConf.getDBDriver().equals(CDAConfig.POSTGRESQL_DRIVER)) {
			Connection conn = DriverManager.getConnection(workConf.getDBConnection(), workConf.getDBUsername(),
					workConf.getDBPassword());
			Catalog.setSeparator(conn);
			return (conn);
		} else {
			return null;
		}
	}

	/**
	 * Make MongoDB connection
	 * 
	 * @return MongoClient object (equivalent to Connection object)
	 */
	public final MongoDatabase makeMongoDBConnection() {
		String connString = this.workConf.getDBConnection();
		MongoClient mongoClient = MongoClients.create(connString);
		MongoDatabase database = mongoClient.getDatabase(this.workConf.getDBName());

		return (database);
	}

	/**
	 * Make CouchDB connection
	 * 
	 * @return HttpClient object (equivalent to Connection object)
	 * @throws IOException
	 */
	public final HttpClient makeCouchDBConnection() throws IOException {
		HttpClient httpClient = HttpClient.newBuilder().version(Version.HTTP_1_1).followRedirects(Redirect.NORMAL)
				.connectTimeout(Duration.ofSeconds(20)).build();
		return httpClient;
	}

	/**
	 * Make ExistDB connection
	 * 
	 * @return Collection object (equivalent to Connection object)
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws SecurityException
	 * @throws NoSuchMethodException
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 * @throws InstantiationException
	 * @throws XMLDBException
	 */
	public final Collection makeExistDBConnection() throws IOException, ClassNotFoundException, InstantiationException,
			IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
			SecurityException, XMLDBException {
		Class<?> cl = Class.forName(this.workConf.getDBDriver());
		Database database = (Database) cl.getDeclaredConstructor().newInstance();
		database.setProperty("create-database", "true");
		// TODO: Funktioniert das mit dem DatabaseManager wirklich so??
		DatabaseManager.registerDatabase(database);
		Collection collection = DatabaseManager.getCollection(workConf.getDBConnection() + "/cda",
				workConf.getDBUsername(), workConf.getDBPassword());
		return collection;
	}

}
