import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.Date;

public class Main {

    private static Connection conn;
    private static Statement stmt;

    public static void connection() throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        conn = DriverManager.getConnection("jdbc:sqlite:EURUSD_D1.db");
        stmt = conn.createStatement();
    }

    public static void insert_db(String csv_name,String table) throws IOException {

        FileReader file = null;
        try {
            file = new FileReader(csv_name); //"EURUSD1440.csv"
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        BufferedReader br = new BufferedReader(file);
        String str;
        OHLC jj = new OHLC();

        java.sql.Date sqld;
        java.sql.Time sqlt;

        while ((str=br.readLine())!=null) {

            try {
                PreparedStatement prs = conn.prepareStatement("INSERT INTO "+table +" (Date,Time, Open,High,Law,Close) " +
                        "VALUES (?,?,?,?,?,?)");
                jj = getInfo(str);
                sqld = new java.sql.Date(jj.date.getTime());
                sqlt = new java.sql.Time(jj.date.getTime());
                prs.setDate(1, sqld);
                prs.setTime(2, sqlt);
                prs.setDouble(3, jj.o);
                prs.setDouble(4, jj.h);
                prs.setDouble(5, jj.l);
                prs.setDouble(6, jj.c);
                int rs = prs.executeUpdate();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }
    }

    public static void disconnect() throws SQLException {
        conn.close();
    }

    public static OHLC getInfo(String s) {
        OHLC sv;
        sv = new OHLC();
        String str;
        str = s.substring(0,16);
        try {
            sv.date = new SimpleDateFormat("yyyy.MM.dd,hh:mm").parse(str);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        str = s.substring(17,24);
        sv.o = Double.parseDouble(str);
        str = s.substring(25,32);
        sv.h = Double.parseDouble(str);
        str = s.substring(33,40);
        sv.l = Double.parseDouble(str);
        str = s.substring(41,48);
        sv.c = Double.parseDouble(str);
        System.out.println(sv.date + " " + sv.o +" "+ sv.h+" "+ sv.l+" "+sv.c);
        return sv;

    }

    public static void getCandle(String table, String date, boolean tf) throws SQLException, ParseException {
        // true - d1, false - h4
        ResultSet rs = null;
        Date rr = new SimpleDateFormat("yyyy.MM.dd,hh:mm").parse(date);
        System.out.println(rr);
        String h4;
        if (tf) h4=""; else h4=" AND Time=" + rr.getTime();
        try {
            rs = stmt.executeQuery("SELECT * FROM "+table+" WHERE Date=" + rr.getTime()+h4);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        while (rs.next()) {
            System.out.println(rs.getInt(1) + " " + rs.getDate("Date") + " " + rs.getTime(3)
                    + " " + rs.getDouble(4)+ " " + rs.getDouble(5)+ " " + rs.getDouble(6)+ " " + rs.getDouble(7));
        }

    }

    public static void main(String[] args) throws SQLException, ParseException {
        try {
            connection();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

      /*try {
            insert_db("EURUSD240.csv","EURUSD_H4");
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        /*Date ll = new Date(1158159600000l);
        System.out.println(ll);*/

        getCandle("EURUSD_H4","2010.12.02,08:00",false);

        disconnect();


    }
}
