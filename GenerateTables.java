package Javafinal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement.*;
import java.sql.ResultSet;
import java.sql.*;
import java.io.*;
import javafx.application.Application;

import java.util.*;

public class GenerateTables {
    
    // TABLES
    static void createTables(Connection conn) throws Exception {
        PreparedStatement stmt;

        //BASE TABLE
        //stmt = conn.prepareStatement("CREATE TABLE Base(TRIPDURATION INT(8), START_STATION_ID int, START_STATION_NAME varchar(50), START_STATION_LATITUDE float(12,6), START_STATION_LONGITUDE float(12,6), END_STATION_ID int, END_STATION_NAME varchar(50), END_STATION_LATITUDE float(12,6), END_STATION_LONGITUDE float(12,6), BIRTHYEAR int(4), GENDER int(1), startDay varchar(15), endDay varchar(15));");
        //stmt.execute();

        // STATIONS
        stmt = conn.prepareStatement("CREATE TABLE Stations(ID int PRIMARY KEY NOT NULL UNIQUE , NAME varchar(30), latitude FLOAT(12,6), longitude FLOAT(12,6));");
        stmt.execute();

        //TRIPS
        stmt = conn.prepareStatement("CREATE TABLE Trips(stationID int NOT NULL, minTripDuration INT(8), maxTripDuration INT(8), avgTripDuration INT(8), NumberOfUsers INT(6));");
        stmt.execute();

        //USAGEBYDAY
        stmt = conn.prepareStatement("CREATE TABLE UsageByDay(stationID int UNIQUE NOT NULL, numberUsersWeekday INT(8), numberUsersWeekend INT(8));");
        stmt.execute();

        //USAGEBYGENDER 
        //make sure to specify, if gender ==0, then skip add
        stmt = conn.prepareStatement("CREATE TABLE UsageByGender(stationID int UNIQUE NOT NULL, numberMaleUsers INT(8), numberFemaleUsers INT(8));");
        stmt.execute();

        //USAGEBYAGE
        //make sure to specify, if year == \n || year ==null, skip add
        stmt= conn.prepareStatement("CREATE TABLE UsageByAge(stationID int NOT NULL, numberUsersUnder18 INT(8), numberUsers18to40 INT(8), numberUsersOver40 INT(8));");
        stmt.execute();
    } 

    static void add_base(Connection conn, int tripdur, int startstationid, String startstationname, float startlatitude, float startlongitude, int endstationid, String endstationname, float endlatitude, float endlongitude, int year, int gen, String startd, String endd) throws Exception {
        PreparedStatement inputStmt;
        String insrt;
        insrt = String.format("INSERT INTO Base(TRIPDURATION, START_STATION_ID, START_STATION_NAME, START_STATION_LATITUDE, START_STATION_LONGITUDE, END_STATION_ID, END_STATION_NAME, END_STATION_LATITUDE, END_STATION_LONGITUDE, BIRTHYEAR, GENDER, STARTDAY, ENDDAY) VALUES('%d','%d','%s', '%f', '%f', '%d', '%s', '%f', '%f', '%d', '%d', '%s', '%s');", tripdur, startstationid, startstationname, startlatitude, startlongitude, endstationid, endstationname, endlatitude, endlongitude, year, gen, startd, endd);
        inputStmt = conn.prepareStatement(insrt);
        inputStmt.executeUpdate();

    }

    static void add_station(Connection conn, int ID, String name, float latitude, float longitude) throws Exception {
        PreparedStatement inputStmt;
        String insrt;
        insrt = String.format("INSERT IGNORE INTO Stations(ID, NAME, LATITUDE, LONGITUDE) VALUES('%d', '%s', '%f', '%f');", ID, name, latitude, longitude);
        inputStmt = conn.prepareStatement(insrt);
        inputStmt.executeUpdate();
    }

    static void add_trip(Connection conn, int statID, int mintripdur, int maxtripdur, int avgtripdur, int numusers) throws Exception {
        PreparedStatement inputStmt;
        String insrt;
        insrt = String.format("INSERT IGNORE INTO Trips(stationID, minTripDuration, maxTripDuration, avgTripDuration, NumberOfUsers) VALUES('%d', '%d', '%d', '%d', '%d');", statID, mintripdur, maxtripdur, avgtripdur, numusers);
        inputStmt = conn.prepareStatement(insrt);
        inputStmt.executeUpdate();
    }

    static void add_usageByDay(Connection conn, int statID, int numusersweekday, int numusersweekend) throws Exception {
        PreparedStatement inputStmt;
        String insrt;
        insrt = String.format("INSERT IGNORE INTO UsageByDay(stationID, numberUsersWeekday, numberUsersWeekend) VALUES('%d', '%d', '%d');", statID, numusersweekday, numusersweekend);
        inputStmt = conn.prepareStatement(insrt);
        inputStmt.executeUpdate();
    }

    static void add_usageByGender(Connection conn, int statID, int nummales, int numfemales) throws Exception {
        PreparedStatement inputStmt;
        String insrt;
        insrt = String.format("INSERT IGNORE INTO UsageByGender(stationID, numberMaleUsers, numberFemaleUsers) VALUES('%d', '%d', '%d');", statID, nummales, numfemales);
        inputStmt = conn.prepareStatement(insrt);
        inputStmt.executeUpdate();
    }

    static void add_usageByAge(Connection conn, int statID, int numusers18, int numusers1840, int numusers40) throws Exception {
        PreparedStatement inputStmt;
        String insrt;
        insrt = String.format("INSERT INTO UsageByAge(stationID, numberUsersUnder18, numberUsers18to40, numberUsersOver40) VALUES('%d', '%d', '%d', '%d');", statID, numusers18, numusers1840, numusers40);
        inputStmt = conn.prepareStatement(insrt);
        inputStmt.executeUpdate();
    }

    static void dayInput(Connection conn) throws Exception {
        // weekend
        PreparedStatement stmt = conn.prepareStatement("SELECT START_STATION_ID, count(startDay) from base where startDay = 'Saturday' or startDay = 'Sunday' group by start_station_id;");
        ResultSet row = stmt.executeQuery();
        int placeholder = 0;
        while(row.next()) {
            int stationIDs = row.getInt("START_STATION_ID");
            int WeekendCount = row.getInt("COUNT(STARTDAY)");
            add_usageByDay(conn, stationIDs, placeholder, WeekendCount);
        }
        row.close();

        // weekday
        stmt = conn.prepareStatement("SELECT START_STATION_ID, count(startDAY) from base where startDay NOT IN ('Saturday', 'Sunday') group by start_station_id;");
        row = stmt.executeQuery();
        while(row.next()) {
            int stationIDE = row.getInt("START_STATION_ID");
            int WeekdayCount = row.getInt("COUNT(startDAY)");
            PreparedStatement updateStmt;
            String update;
            update = String.format("UPDATE usagebyday SET numberUsersWeekday = numberUsersWeekday + '%d' WHERE stationID = '%d';", WeekdayCount, stationIDE);
            updateStmt = conn.prepareStatement(update);
            updateStmt.executeUpdate();            
        }
        row.close();
    }
    static void stationInput(Connection conn) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("SELECT START_STATION_ID, START_STATION_NAME, START_STATION_LATITUDE, START_STATION_LONGITUDE from base GROUP BY START_STATION_ID;");
        ResultSet row = stmt.executeQuery();
        while(row.next()) {
            int stationIDs = row.getInt("START_STATION_ID");
            String stationNameS = row.getString("START_STATION_NAME");
            float stationlats = row.getFloat("START_STATION_LATITUDE");
            float stationlongs = row.getFloat("START_STATION_LONGITUDE");
            add_station(conn, stationIDs, stationNameS, stationlats, stationlongs);
        }
        row.close();
        /*
        int stationIDE = row.getInt("END_STATION_ID");
            String stationNameE = row.getString("END_STATION_NAME");
            float stationlate = row.getFloat("END_STATION_LATITUDE");
            float stationlonge = row.getFloat("END_STATION_LONGITUDE");
            add_station(conn, stationIDE, stationNameE, stationlate, stationlonge); */
    }

    static void tripInput(Connection conn) throws Exception {
        // START STATIONS:
        PreparedStatement stmt = conn.prepareStatement("SELECT START_STATION_ID, AVG(TRIPDURATION), MIN(TRIPDURATION), MAX(TRIPDURATION) FROM base GROUP BY START_STATION_ID;");
        ResultSet row = stmt.executeQuery();
        while(row.next()) {
            int stationIDs = row.getInt("START_STATION_ID");
            int tripavg = row.getInt("AVG(TRIPDURATION)");
            int tripmin = row.getInt("MIN(TRIPDURATION)");
            int tripmax = row.getInt("MAX(TRIPDURATION)");
            int holdervalue = 0;
            add_trip(conn, stationIDs, tripmin, tripmax, tripavg, holdervalue);
        }
        row.close();

        // END STATIONS: GETS TRICKY HERE, CUS ONLY UPDATE IF MIN/AVG/MAX IS TRUE MIN/AVG/MAX


        // ADDING USERS START STATION
       PreparedStatement stmt3 = conn.prepareStatement("select start_station_id, count(Start_Station_ID) from base group by start_station_id;");
       ResultSet row3 = stmt3.executeQuery();
       while(row3.next()) {
            int stationID = row3.getInt("START_STATION_ID");
            int usercount = row3.getInt("count(start_station_id)");
            PreparedStatement updateStmt;
            String update;
            update = String.format("UPDATE trips SET numberofusers = Numberofusers + '%d' WHERE stationID = '%d';", usercount, stationID);
            updateStmt = conn.prepareStatement(update);
            updateStmt.executeUpdate();
       }
       row3.close();

       // ADDING USERS END STATION
       PreparedStatement stmt4 = conn.prepareStatement("select end_station_id, count(end_Station_ID) from base group by end_station_id;");
       ResultSet row4 = stmt4.executeQuery();
       while(row4.next()) {
            int stationID = row4.getInt("END_STATION_ID");
            int usercount = row4.getInt("count(end_station_id)");
            PreparedStatement updateStmt;
            String update;
            update = String.format("UPDATE trips SET numberofusers = numberofusers + '%d' WHERE stationID = '%d';", usercount, stationID);
            updateStmt = conn.prepareStatement(update);
            updateStmt.executeUpdate();
       }
       row4.close();

    }

    /* static void UsageByDayInput(Connection conn) throws Exception {
    // have to edit base upload for this to include start and end times so we can do it by day

    }*/

    static void UsageByGenderInput(Connection conn) throws Exception {
        // males
        PreparedStatement stmt = conn.prepareStatement("select start_station_id, count(start_station_id) from base where gender = 1 group by start_station_id;");
        ResultSet row = stmt.executeQuery();
        int placeholder = 0;
        while(row.next()) {
            int stationID = row.getInt("start_station_id");
            int males = row.getInt("count(start_station_id)");
            add_usageByGender(conn, stationID, males, placeholder);
        }
        row.close();

        stmt = conn.prepareStatement("select end_station_id, count(end_station_id) from base where gender = 1 group by end_station_id;");
        row = stmt.executeQuery();
        while(row.next()) {
            int stationID = row.getInt("end_station_id");
            int males = row.getInt("count(end_station_id)");
            PreparedStatement updateStmt;
            String update;
            update = String.format("UPDATE usagebygender SET numberMaleUsers = numberMaleUsers + '%d' WHERE stationID = '%d';", males, stationID);
            updateStmt = conn.prepareStatement(update);
            updateStmt.executeUpdate();
        }
        row.close();

        // females UPDATE
        stmt = conn.prepareStatement("select start_station_id, count(start_station_id) from base where gender = 2 group by start_station_id;");
        row = stmt.executeQuery();
        while(row.next()) {
            int stationID = row.getInt("start_station_id");
            int females = row.getInt("count(start_station_id)");
            PreparedStatement updateStmt;
            String update;
            update = String.format("UPDATE usagebygender SET numberFemaleUsers = numberFemaleUsers + '%d' WHERE stationID = '%d';", females, stationID);
            updateStmt = conn.prepareStatement(update);
            updateStmt.executeUpdate();
        }
        row.close();

        stmt = conn.prepareStatement("select end_station_id, count(end_station_id) from base where gender = 2 group by end_station_id;");
        row = stmt.executeQuery();
        while(row.next()) {
            int stationID = row.getInt("end_station_id");
            int females = row.getInt("count(end_station_id)");
            PreparedStatement updateStmt;
            String update;
            update = String.format("UPDATE usagebygender SET numberFemaleUsers = numberFemaleUsers + '%d' WHERE stationID = '%d';", females, stationID);
            updateStmt = conn.prepareStatement(update);
            updateStmt.executeUpdate();
        }
        row.close();

    }

    /* for age, 2013 is the year the data was collected.
    So the Age Break Down is as follows:
    <18 : 1996 > 
    18<x<40 : 1995 - 1973
    >40 : 1973 < AND NOT == ZERO */

    static void UsageByAgeInput(Connection conn) throws Exception {
        // < 18
        PreparedStatement stmt = conn.prepareStatement("select start_station_id, count(start_station_id) from base where birthyear between 1973 and 1995 group by start_station_id;");
        ResultSet row = stmt.executeQuery();
        int placeholder = 0;
        while(row.next()) {
            int stationID = row.getInt("start_station_id");
            int middle = row.getInt("count(start_station_id)");
            add_usageByAge(conn, stationID, placeholder, middle, placeholder);
        }
        row.close();

        stmt = conn.prepareStatement("select end_station_id, count(end_station_id) from base where birthyear > 1995 group by end_station_id;");
        row = stmt.executeQuery();
        while(row.next()) {
            int stationID = row.getInt("end_station_id");
            int young = row.getInt("count(end_station_id)");
            PreparedStatement updateStmt;
            String update;
            update = String.format("UPDATE usagebyage SET numberUsersUnder18 = numberUsersUnder18 + '%d' WHERE stationID = '%d';", young, stationID);
            updateStmt = conn.prepareStatement(update);
            updateStmt.executeUpdate();
        }
        row.close();

        stmt = conn.prepareStatement("select end_station_id, count(end_station_id) from base where birthyear < 1973 group by end_station_id;");
        row = stmt.executeQuery();
        while(row.next()) {
            int stationID = row.getInt("end_station_id");
            int older = row.getInt("count(end_station_id)");
            PreparedStatement updateStmt;
            String update;
            update = String.format("UPDATE usagebyage SET numberUsersOver40 = numberUsersOver40 + '%d' WHERE stationID = '%d';", older, stationID);
            updateStmt = conn.prepareStatement(update);
            updateStmt.executeUpdate();
        }
        row.close();        
    }

    static void usagePatterns(Connection conn, int stationinput ) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("select startDay, gender, count(gender), avg(birthyear), count(startDay) from base" + " where start_station_id = " + stationinput + " group by gender, startDay;");
        ResultSet row = stmt.executeQuery();
        while(row.next()) {
            String startDay = row.getString("startDay");
            int genderID = row.getInt("gender");
            String gender;
            if (genderID == 0) {
                gender = "Unknown";
            } if (genderID == 1) {
                gender = "Male";
            } else {
                gender = "Female";
            }
            int genderCount = row.getInt("count(gender)");
            int avgBirthYear = row.getInt("avg(birthyear)");
            int freqDay = row.getInt("count(startDay)");
            System.out.println("Day of Week: " + startDay + " Gender: " + gender + " Frequency of Gender: " + genderCount + " Average Birth Year of Riders: " + avgBirthYear + " Amount of Riders This Day: " + freqDay);
        }
        row.close();
    }

    static void mostFreqTripDay(Connection conn, String day) throws Exception {
        PreparedStatement stmt = conn.prepareStatement("select TRIP_ID, MAX(TRIP_FREQ) from (select TRIP_ID, COUNT(*) as TRIP_FREQ from (select CONCAT(START_STATION_NAME, END_STATION_NAME) as TRIP_ID from base " + " where startDay = '" + day + "' ) as UNIQUE_TRIPS GROUP BY TRIP_ID) as UNIQUE_TRIP_FREQ;");
        ResultSet row = stmt.executeQuery();
        while(row.next()) {
            String tripID = row.getString("TRIP_ID");
            int tripfreq = row.getInt("MAX(TRIP_FREQ)");
            System.out.println("MOST FREQUENT TRIP FOR THE DAY OF " + day + " : " + tripID + ". Frequency of Trip: " + tripfreq);
        }
        row.close();
    }
}