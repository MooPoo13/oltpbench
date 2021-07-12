package de.ukr.benchmarks.cdabench;

import java.text.SimpleDateFormat;

public class CDAConfig {
	
	public static enum TransactionType {
		INVALID, // Exists so the order is the same as the constants below
		DISEASE_WOMEN_AGE_RANGE, 
		PROCEDURE_TIME_RANGE,
		PROCEDURE_WOMEN_AGE_RANGE, 
		DISEASE,
		MEDICATION_AGE
	}
	
	public final static String COUCHDB_DRIVER    = "couch-driver"; 
	public final static String EXISTDB_DRIVER    = "org.exist.xmldb.DatabaseImpl"; 
	public final static String MONGODB_DRIVER    = "com.mongodb.client.MongoClients"; 
	public final static String POSTGRESQL_DRIVER = "org.postgresql.Driver"; 

	public final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	// Different item counts to benchmark
	public final static int configItemCount1k   =     1000; //      1,000
	public final static int configItemCount10k  =    10000; //     10,000
	public final static int configItemCount100k =   100000; //    100,000
	public final static int configItemCount1m   =  1000000; //  1,000,000
	public final static int configItemCount10m  = 10000000; // 10,000,000
	
	public final static int configItemCount = configItemCount1k;
	
	// Query 1
	public final static int procedureMinAge = 20; // min age of patients for Q1
	public final static int procedureMaxAge = 50; // max age of patients for Q1
	public final static String patientSex = "female"; // patient sex for Q1
	
	// Query 2
	public final static String procedureCodeQ2 = ""; // code of procedure for Q2
	public final static int procedureYear = 2020; // year of procedures for Q2
	
	// Query 3
	public final static String procedureCodeQ3 = ""; // code of procedure for Q3
	public final static int procedureTimeRangeQ3 = 5; // time range in years for procedures for Q3
	
	// Query 4
	public final static String diseaseCode = ""; // code of disease for Q4
	
	// Query 5
	public final static String medicationName= ""; // name of medication for Q5
	public final static String medicationCode = ""; // code of medication for Q5
}
