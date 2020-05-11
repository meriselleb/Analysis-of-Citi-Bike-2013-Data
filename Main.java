package Javafinal;
import java.sql.*;
import javafx.scene.*;
import javafx.application.Application;
import javafx.application.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.io.*;
import java.util.*;
import java.lang.Object.*;
import java.util.Scanner;
import java.lang.Object.*;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;


public class Main{
    public static void main(String[] args) {

        try {
            Class.forName("com.mysql.jdbc.Driver");

            // setting up connection
            Connection conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/java_final?useSSL=false", "roots", "roots");

            // find a way to do, IF TABLE EXISTS, DROP // NO BASE;
            // PreparedStatement stmt = conn.prepareStatement("DROP TABLE usagebyday");
            //stmt.execute(); 

            System.out.println("Making database.");
            //GenerateTables.createTables(conn);
            System.out.println("Database made.");

            //ReadData.LoadData(conn);
            System.out.println("Base Table Generated.");

            // STATION INPUT;
            //GenerateTables.stationInput(conn);
            System.out.println("Station Table Filled.");

            //GenerateTables.UsageByAgeInput(conn);
            System.out.println("UsageByAge Table Filled.");

            //GenerateTables.UsageByGenderInput(conn);
            System.out.println("Gender Table Filled.");

            //GenerateTables.tripInput(conn);
            System.out.println("Trip Table Filled.");

            //GenerateTables.dayInput(conn);
            System.out.println("Usage By Day Table Filled.");
            GenerateTables.usagePatterns(conn, 2022);
            String day = "Saturday";
            GenerateTables.mostFreqTripDay(conn,day );


        }
        catch(Exception e) {
            System.out.println("Exception caught here: " + e);
        }
    }
}