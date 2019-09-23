package ms3Challenge;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Scanner;
import java.io.*;



/*this class should extract information from a CSV and 
write the entries to an SQLite database, 
taking all malformed entries and writing them to a new CSV file and 
recording statistics to a log*/

public class CsvToSqlite {

	
	public static void main(String[] args) {
	
		//Get filename from user
		System.out.println("Please enter the file path");
		Scanner reqFilePath = new Scanner(System.in);
		String filePath = reqFilePath.nextLine();
		if(!filePath.contains(".csv"))
			filePath += ".csv";
		File inputFile = new File(filePath);
		String fileToParse = inputFile.getName();
		reqFilePath.close();
		
		
        BufferedReader fileReader = null;
        
        //create SQLite DB
        String dbName = fileToParse.subSequence(0, fileToParse.indexOf("."))+"";
        createNewDb(dbName);
         
        //Delimiter used in CSV file
        final String DELIMITER = ",";
        
        //ints to track successful records and failed records
        int sRecords = 0, fRecords = 0;
        
        try
        {
            String line = "";
            
            //Create the file reader
            fileReader = new BufferedReader(new FileReader(filePath));
            
            //Create bad records CSV file
            FileWriter badRecords = new FileWriter(fileToParse.subSequence(0, fileToParse.indexOf("."))+"-bad.csv");
            
            //intialize headers 
            line = fileReader.readLine();
            String[] tokens = line.split(DELIMITER);
            for(String token : tokens) {
            	badRecords.append(token + ",");
            	}
            badRecords.append(System.lineSeparator());
            
            //Create Table in Sqlitedb
            createDb10ColTable(dbName);
            
            
            //Read the file line by line
            while ((line = fileReader.readLine()) != null)
            {
                //Get all tokens available in line
                tokens = line.split(DELIMITER);
                if(tokens.length < 11) {
                	for(String token : tokens) {
                	badRecords.append(token + ",");
                	}
                	badRecords.append(System.lineSeparator());
                	fRecords++;
                }
                else {
                	
                		//Write to SQlite db
                		insertDb10Col(dbName, tokens);
                		sRecords++;
                		
                	
                }
            }
            badRecords.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally
        {
            try {
            	//create and write log file
            	FileWriter statLog = new FileWriter(fileToParse.subSequence(0, fileToParse.indexOf("."))+".log");
            	int totRecords = sRecords + fRecords;
            	statLog.write("# of records received: " + totRecords + System.lineSeparator() +
            			"# of records successful: " + sRecords + System.lineSeparator() +
            			"# of records failed: " + fRecords);
            	
            	fileReader.close();
                statLog.close();
                System.out.println("The process is complete.");
                
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
 

	}

	public static void createNewDb(String fileName) {
		 
        String url = "jdbc:sqlite:" + fileName + ".db";
 
        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                conn.close();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database, " + fileName + ", has been created.");
            }
 
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
	
	
	public static void createDb10ColTable(String fileName) {
		 
        String url = "jdbc:sqlite:" + fileName + ".db";
 
        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                Statement stmt = conn.createStatement();
                String cmd = "CREATE TABLE IF NOT EXISTS " + fileName +
                		" (A TEXT NOT NULL," +
                		" B TEXT NOT NULL, " +
                		" C TEXT NOT NULL, " +
                		" D TEXT NOT NULL, " +
                		" E TEXT NOT NULL, " +
                		" F TEXT NOT NULL, " +
                		" G TEXT NOT NULL, " +
                		" H TEXT NOT NULL, " +
                		" I TEXT NOT NULL, " +
                		" J TEXT NOT NULL) " ;
                stmt.executeUpdate(cmd);
                stmt.close();
                conn.close();
                System.out.println("The table has been created.");
            }
 
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }

	public static void insertDb10Col(String fileName, String[] entry) {
		String url = "jdbc:sqlite:" + fileName + ".db";
		 
        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                Statement stmt = conn.createStatement();
                String cmd = "INSERT INTO " + fileName + " (A,B,C,D,E,F,G,H,I,J) " + 
                			"VALUES ("  +"'"+entry[0]+"'"+ ","
                						+"'"+entry[1]+"'"+ ","
                						+"'"+entry[2]+"'"+ ","
                						+"'"+entry[3]+"'"+ ","
                						+"'"+entry[4]+"'"+ ","
                						+"'"+entry[5]+"'"+ ","
                						+"'"+entry[6]+"'"+ ","
                						+"'"+entry[7]+"'"+ ","
                						+"'"+entry[8]+"'"+ ","
                						+"'"+entry[9]+"'"+")";
                stmt.executeUpdate(cmd);
                stmt.close();
                conn.close();
                System.out.println("The entry " + entry[0] + " has been added.");
            }
 
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
	}
	
}
