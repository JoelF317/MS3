package ms3Challenge;


import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
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
		String fileToParse = reqFilePath.nextLine();
		if(!fileToParse.contains(".csv"))
			fileToParse += ".csv";
		File inputFile = new File(fileToParse);
		fileToParse = inputFile.getName();
		reqFilePath.close();
		
        BufferedReader fileReader = null;
        createNewDb(fileToParse);
         
        //Delimiter used in CSV file
        final String DELIMITER = ",";
        
        //ints to track successful records and failed records
        int sRecords = 0, fRecords = 0;
        
        try
        {
            String line = "";
            
            //Create the file reader
            fileReader = new BufferedReader(new FileReader(inputFile.getPath()));
            
            //Create bad records CSV file
            FileWriter badRecords = new FileWriter(fileToParse.subSequence(0, fileToParse.indexOf("."))+"-bad.csv");
            
            
            //Read the file line by line
            while ((line = fileReader.readLine()) != null)
            {
                //Get all tokens available in line
                String[] tokens = line.split(DELIMITER);
                if(tokens.length < 10) {
                	badRecords.write(line);
                	fRecords++;
                }
                else {
                	for(String token : tokens)
                	{
                		//Write to SQlite db
                		sRecords++;
                		
                	}
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
            	statLog.write("# of records received: " + totRecords + "/n" +
            			"# of records successful: " + sRecords + "/n" +
            			"# of records failed: " + fRecords);
            	
            	fileReader.close();
                statLog.close();
                
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
 

	}

	public static void createNewDb(String fileName) {
		 
        String url = "jdbc:sqlite:C:/sqlite/" + fileName;
 
        try (Connection conn = DriverManager.getConnection(url)) {
            if (conn != null) {
                DatabaseMetaData meta = conn.getMetaData();
                System.out.println("The driver name is " + meta.getDriverName());
                System.out.println("A new database has been created.");
            }
 
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
	
}
