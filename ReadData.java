package Javafinal;
import java.io.*;
import java.sql.*;
import java.lang.Object.*;
import java.text.SimpleDateFormat;
import java.text.DateFormat;
import java.util.Date;


public class ReadData {
    static void LoadData(Connection conn) throws Exception {
        String pathToCsv = "C:/workspace/java/javafinal/CitiBikeData.csv";           // DEFINE LATER
        BufferedReader csvReader = new BufferedReader(new FileReader(pathToCsv));
        int count = 0;
        String row;
        while ((row = csvReader.readLine()) != null) {
            if (count == 0) {
                count++;
            } else {
                count++;
                String[] data = row.split(",");
                // BASE TABLE PARSING:
                int tripdur = Integer.parseInt(data[0]);
                int startstationid = Integer.parseInt(data[5]);
                // start day
                String startDatePre = data[1].substring(0, 8);
                SimpleDateFormat format1 = new SimpleDateFormat("dd/MM/yyyy");
                Date dt1 = format1.parse(startDatePre);
                DateFormat format2 = new SimpleDateFormat("EEEE"); 
                String startDay = format2.format(dt1);
                // end day
                String endDatePre = data[3].substring(0, 8);
                format1 = new SimpleDateFormat("dd/MM/yyyy");
                dt1 = format1.parse(endDatePre);
                format2 = new SimpleDateFormat("EEEE"); 
                String endDay = format2.format(dt1);

                String startstationname = data[6];
                Float startstationlat = Float.parseFloat(data[7]);
                Float startstationlong = Float.parseFloat(data[8]);
                int endstationid = Integer.parseInt(data[9]);
                String endstationname = data[10];
                Float endstationlat = Float.parseFloat(data[11]);
                Float endstationlong = Float.parseFloat(data[12]);
                int birth = Integer.parseInt(data[15]);
                int gen = Integer.parseInt(data[16]);
                GenerateTables.add_base(conn, tripdur, startstationid, startstationname, startstationlat, startstationlong, endstationid, endstationname, endstationlat, endstationlong, birth, gen, startDay, endDay);
            }
        }
        csvReader.close();
    }
}    
