package JCDBCHW;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws SQLException {
        
        Connection dbConnection = null;
        Savepoint savepoint = null;
        try {
        	System.out.println("Establishing database connection...");
            // Establishing the database connection
            dbConnection = getConnection();
            
            System.out.println("Reading data from all tables...");
            // Reading data from all tables
            ReadAllData(dbConnection);

            System.out.println("Executing prepared statements...");
            // Executing prepared statements
            preparedStatements(dbConnection);
            
            Statement stmt = dbConnection.createStatement();
	        // Inserting data into the table
	        String sql1 = "insert into Employee values (75, 'Erikson', 'Mia',  '1604 Highland Dr.', 'Grove', 'FL', 33321, '1970-09-19', '1998-02-04', 2, NULL, NULL)";
	        stmt.executeUpdate(sql1);
	        dbConnection.commit();

            // Creating a savepoint for rollback
            savepoint = dbConnection.setSavepoint("Savepoint");

            //insert more data
            String sql2 = "insert into Employee values (40, 'Katz', 'Shia',  '123 Mayflower Road.', 'Fillmore', 'FL', 33336, 2003-06-13, 2024-01-23, 'Three', NULL, NULL)";
			stmt.executeUpdate(sql2);
			dbConnection.commit();

            System.out.println("Closing the database connection...");
            // Closing the database connection
            dbConnection.close();
        } catch (SQLException sqlE) {
        	System.out.println("An error occurred. Rolling back to the savepoint...");
            // Rolling back to the savepoint in case of exception
            try {
                if (dbConnection != null && savepoint != null) {
                    dbConnection.rollback(savepoint);
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }

            try {
                Statement stmt = dbConnection.createStatement();
                ResultSet resultSet = stmt.executeQuery("SELECT * FROM Employee");

                // Getting metadata
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                // Printing table name
                System.out.println("Employee table contents:");
                System.out.print("Table: Employee\n");

                // Printing column names
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(metaData.getColumnName(i) + "\t");
                }
                System.out.println();

                // Printing rows
                while (resultSet.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(resultSet.getString(i) + "\t");
                    }
                    System.out.println();
                }
                resultSet.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }

            // Close the connection within the catch block
            try {
                if (dbConnection != null && !dbConnection.isClosed()) {
                    dbConnection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
    
    // Method to read data from all tables
    public static void ReadAllData(Connection connection) {
        try {
            // SQL query to select data from multiple tables
            final String SELECT_QUERY = "SELECT * FROM CUSTOMER; SELECT * FROM Employee; "
                    + "SELECT * FROM ORDER_LINE; SELECT * FROM ORDERS; SELECT * FROM PART; SELECT * FROM SALESREP";
            Statement statement = connection.createStatement();

            // Splitting the SELECT_QUERY into individual queries
            String[] queries = SELECT_QUERY.split(";");

            // Executing each query separately
            for (String query : queries) {
                // Remove leading and trailing whitespace
                query = query.trim();

                if (!query.isEmpty()) {
                    // Extracting the table name from the query
                    String tableName = query.substring(query.indexOf("FROM") + 5).trim();
                    tableName = tableName.split("\\s+")[0]; // Extract the table name

                    // Printing the table name
                    System.out.println("Executing query: " + query);
                    System.out.println("Table: " + tableName);
                    
                    // Executing the query
                    boolean hasResults = statement.execute(query);

                    // Processing the result set
                    if (hasResults) {
                        ResultSet resultSet = statement.getResultSet();
                        ResultSetMetaData metaData = resultSet.getMetaData();
                        int numberOfColumns = metaData.getColumnCount();

                        // Displaying column names
                        System.out.println("Columns and results for table " + tableName + ":");
                        // Displaying column names
                        for (int i = 1; i <= numberOfColumns; i++) {
                            System.out.printf("%-16s\t", metaData.getColumnName(i));
                        }
                        System.out.println();

                        // Displaying query results
                        while (resultSet.next()) {
                            for (int i = 1; i <= numberOfColumns; i++) {
                                System.out.printf("%-8s (%-8s)\t", resultSet.getObject(i),
                                        metaData.getColumnTypeName(i));
                            }
                            System.out.println();
                        }
                        System.out.println();
                    }
                }
            }
        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }
    
    private static Connection getConnection() throws SQLException {
        // Database connection URL
        final String DATABASE_URL = "jdbc:sqlserver://localhost;" + "databaseName=PREMIERECO;"
                + "integratedSecurity=true;" + "encrypt=true;" + "TrustServerCertificate=true";

        // Database connection properties
        Properties properties = new Properties();
        properties.put("user", "PremiereLogin");
        properties.put("password", "premiereL0g!n");

        // Establishing the database connection
        Connection dbConnection = DriverManager.getConnection(DATABASE_URL, properties);
        // Set auto-commit to false for transaction handling
        dbConnection.setAutoCommit(false);

        // Printing connection status
        System.out.println("Connected!");

        return dbConnection;
    }

    // Method to execute prepared statements
    public static void preparedStatements(Connection connection) throws SQLException {
        // First prepared statement
        PreparedStatement preparedStatement1 = connection.prepareStatement("SELECT * FROM EMPLOYEE WHERE LNAME < ?");
        preparedStatement1.setString(1, "N");
        ResultSet resultSet1 = preparedStatement1.executeQuery();
        System.out.println("Executing prepared statement 1:");
        System.out.println("Employees with last names until N: ");
        printReturnedStatement(resultSet1);

        // Second prepared statement
        PreparedStatement preparedStatement2 = connection.prepareStatement("SELECT * FROM PART WHERE PRICE < ? AND PRICE > ?");
        preparedStatement2.setInt(1, 400);
        preparedStatement2.setInt(2, 200);
        ResultSet resultSet2 = preparedStatement2.executeQuery();
        System.out.println("Executing prepared statement 2:");
        System.out.println("Products with prices between 200 and 400: ");
        printReturnedStatement(resultSet2);

        // Third prepared statement
        PreparedStatement preparedStatement3 = connection.prepareStatement("SELECT * FROM CUSTOMER WHERE CUST_CITY = ?");
        preparedStatement3.setString(1, "Grove");
        ResultSet resultSet3 = preparedStatement3.executeQuery();
        System.out.println("Executing prepared statement 3:");
        System.out.println("Customers who live in Grove city: ");
        printReturnedStatement(resultSet3);

        // Setting parameter value again for the third prepared statement
        preparedStatement3.setString(1, "Fillmore");
        resultSet3 = preparedStatement3.executeQuery();
        System.out.println("Executing prepared statement 3 with change:");
        System.out.println("Customers who live in Fillmore city: ");
        printReturnedStatement(resultSet3);
    }

    // Method to print result set returned by prepared statements
    public static void printReturnedStatement(ResultSet resultSet) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        int numberOfColumns = metaData.getColumnCount();
       
        // Displaying column names
        System.out.println("Column names and query results:");
        for (int i = 1; i <= numberOfColumns; i++) {
            System.out.printf("%-16s\t", metaData.getColumnName(i));
        }
        System.out.println();

        // Displaying query results
        while (resultSet.next()) {
            for (int i = 1; i <= numberOfColumns; i++) {
                System.out.printf("%-8s (%-8s)\t", resultSet.getObject(i), metaData.getColumnTypeName(i));
            }
            System.out.println();
        }
        System.out.println();
    }
}
