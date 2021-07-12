package de.ukr.benchmarks.cdabench.procedures;

import java.net.http.HttpClient;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.bson.Document;
import org.xmldb.api.base.Collection;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.oltpbenchmark.api.Procedure;

import de.ukr.benchmarks.cdabench.CDAWorker;

public abstract class CDAProcedure extends Procedure {

	public abstract ResultSet run(Connection connection, HttpClient httpClient,
			MongoDatabase mongoDatabase, Collection existCollection, CDAWorker worker) throws SQLException;

}
