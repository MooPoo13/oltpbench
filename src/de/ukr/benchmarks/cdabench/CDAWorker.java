package de.ukr.benchmarks.cdabench;

import java.io.IOException;
import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.xmldb.api.base.Collection;

import com.mongodb.client.MongoDatabase;
import com.oltpbenchmark.api.TransactionType;
import com.oltpbenchmark.api.Worker;
import com.oltpbenchmark.api.Procedure.UserAbortException;
import com.oltpbenchmark.types.TransactionStatus;

import de.ukr.benchmarks.cdabench.procedures.CDAProcedure;

public class CDAWorker extends Worker<CDABenchmark> {
	private static final Logger LOG = Logger.getLogger(CDALoader.class);

	protected HttpClient httpClient;
	protected Collection existCollection;
	protected Connection connection;
	protected MongoDatabase mongoDatabase;

	public CDAWorker(CDABenchmark benchmarkModule, int id) {
		super(benchmarkModule, id, false);
		
		// make connection
		String dbType = benchmarkModule.getWorkloadConfiguration().getDBDriver();

		this.existCollection = null;
		this.mongoDatabase = null;
		this.httpClient = null;

		switch (dbType) {
		case CDAConfig.COUCHDB_DRIVER:
			try {
				this.httpClient = this.getBenchmarkModule().makeCouchDBConnection();
			} catch (IOException e) {
				throw new RuntimeException("Failed to connect to CouchDB database", e);
			}

			break;
		case CDAConfig.EXISTDB_DRIVER:
			try {
				this.existCollection = this.getBenchmarkModule().makeExistDBConnection();

			} catch (Exception e) {
				throw new RuntimeException("Failed to connect to ExistDB database", e);
			}
			break;
		case CDAConfig.MONGODB_DRIVER:
			try {
				this.mongoDatabase = this.getBenchmarkModule().makeMongoDBConnection();
			} catch (Exception e) {
				throw new RuntimeException("Failed to connect to ExistDB database", e);
			}
			break;
		case CDAConfig.POSTGRESQL_DRIVER:
			try {
				this.conn = this.getBenchmarkModule().makeConnection();
				this.conn.setAutoCommit(false);

				// 2018-01-11: Since we want to support NoSQL systems
				// that do not support txns, we will not invoke certain JDBC functions
				// that may cause an error in them.
				if (this.wrkld.getDBType().shouldUseTransactions()) {
					this.conn.setTransactionIsolation(this.wrkld.getIsolationMode());
				}
			} catch (SQLException ex) {
				throw new RuntimeException("Failed to connect to database", ex);
			}
			break;
		}

	}

	@Override
	protected TransactionStatus executeWork(TransactionType nextTransaction) throws UserAbortException, SQLException {
		try {
			CDAProcedure proc = (CDAProcedure) this.getProcedure(nextTransaction.getProcedureClass());
			switch (this.getBenchmarkModule().getWorkloadConfiguration().getDBDriver()) {
			case CDAConfig.POSTGRESQL_DRIVER:
				proc.run(conn, null, null, null, this);
				this.conn.commit();
				break;
			case CDAConfig.COUCHDB_DRIVER:
				proc.run(null, this.httpClient, null, null, this);
				break;
			case CDAConfig.MONGODB_DRIVER:
				proc.run(null, null, this.mongoDatabase, null, this);
				break;
			case CDAConfig.EXISTDB_DRIVER:
				proc.run(null, null, null, this.existCollection, this);
				break;
			}

		} catch (ClassCastException ex) {
			// fail gracefully
			LOG.error("We have been invoked with an INVALID transactionType?!");
			throw new RuntimeException("Bad transaction type = " + nextTransaction);
		}

		return (TransactionStatus.SUCCESS);
	}
}
