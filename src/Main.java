import java.io.*;
import java.sql.*;
import java.text.*;
import java.util.Date;

public class Main {

    enum Trend {
        BULL, BEAR, FLAT
    }

    private static Connection conn;
    private static Statement stmt;

    public static void connection() throws ClassNotFoundException, SQLException {
        Class.forName("org.sqlite.JDBC");
        conn = DriverManager.getConnection("jdbc:sqlite:EURUSD_D1.db");
        stmt = conn.createStatement();
    }

    public static void insert_db(String csv_name,String table) throws IOException, SQLException {

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
        int a = 0;
        PreparedStatement prs;
        conn.setAutoCommit(false);

        long tm = System.currentTimeMillis();

       /* while ((str=br.readLine())!=null) {

            try {
                prs = conn.prepareStatement("INSERT INTO "+table +" (Date,Time, Open,High,Law,Close) " +
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
                prs.addBatch();
                a++;
                //int rs = prs.executeUpdate();
                if (a==100) {
                    a=0;
                    prs.executeBatch();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }*/
        //prs.executeBatch();
        prs = conn.prepareStatement("INSERT INTO "+table +" (Date,Time, Open,High,Law,Close) " +
                "VALUES (?,?,?,?,?,?)");

        while ((str=br.readLine())!=null) {

            try {

                jj = getInfo(str);
                sqld = new java.sql.Date(jj.date.getTime());
                sqlt = new java.sql.Time(jj.date.getTime());
                prs.setDate(1, sqld);
                prs.setTime(2, sqlt);
                prs.setDouble(3, jj.o);
                prs.setDouble(4, jj.h);
                prs.setDouble(5, jj.l);
                prs.setDouble(6, jj.c);
                prs.addBatch();
                a++;
                //int rs = prs.executeUpdate();
                if (a==100) {
                    a=0;
                    prs.executeBatch();
                }
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
        }

        prs.executeBatch();
        conn.setAutoCommit(true);
        System.out.println("Время выпонения "+ (System.currentTimeMillis()-tm));
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

    public static OHLC getCandle(String table, String date, boolean tf) throws SQLException, ParseException {
        // true - d1, false - h4
        OHLC r = new OHLC();
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
            r.date = rs.getDate("Date");
            r.o = rs.getDouble(4);
            r.h = rs.getDouble(5);
            r.l = rs.getDouble(6);
            r.c = rs.getDouble(7);
            System.out.println(rs.getInt(1) + " " + r.date + " " + rs.getTime(3)
                    + " " + r.o + " " + r.h + " " + r.l + " " + r.c);
        }
    return r;
    }

    public static OHLC getCandle(String table, int id) throws SQLException, ParseException {
        ResultSet rs = null;
        OHLC r = new OHLC();

        try {
            rs = stmt.executeQuery("SELECT * FROM "+table+" WHERE id=" + id);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        while (rs.next()) {
            r.date = rs.getDate("Date");
            r.o = rs.getDouble(4);
            r.h = rs.getDouble(5);
            r.l = rs.getDouble(6);
            r.c = rs.getDouble(7);
            //System.out.println(rs.getInt(1) + " " + r.date + " " + rs.getTime(3)
            //        + " " + r.o + " " + r.h + " " + r.l + " " + r.c);
        }
    return r;
    }

    public static Trend checkTrend(String table, int id, int period, int percent) throws SQLException, ParseException {
    OHLC c1 = new OHLC();
    c1 = getCandle(table,id);
    double av1 = (c1.l+c1.h)/2;
    c1 = getCandle(table,id-period);
    double av2 = (c1.l+c1.h)/2;
    double per = av2*100/av1;
    //System.out.println(per);
    double res = 100 - per;
    if (res<=-percent) return Trend.BEAR;
    if (res>=percent) return Trend.BULL;
    return Trend.FLAT;
    }

    public static PinBar checkPB(double o, double h, double l, double c) {
        PinBar u = new PinBar();
        double hl30 = (h - l)*0.3;
        if (o>=(h-hl30) & c>=(h-hl30)) {u.pb = true; u.tr = true; return u;};//System.out.println("Верхний пин-бар");
        if (o<=(l+hl30) & c<=(l+hl30)) {u.pb = true; u.tr = false; return u;};//System.out.println("Нижний пин-бар");
        return u;
    }

    public static boolean check0(double h, double l) {
        double t,u;
        t = h * 10000;
        int t1 = (int) (t / 100);
        u = t1/100.0;
        //System.out.println(u);
        if (h>=u & l<=u & ((u*10000) % 100 == 0)) return true;
        return false;
    }

    public static OutSideBar checkOB(double h, double l, double o, double c, double h1, double l1) {
        OutSideBar obar = new OutSideBar();
        obar.ob = false;
        if (h1<h & l1>l) {
            obar.ob = true;
            if (c>o) obar.tr = true;
            return obar;
        }
        return obar;
    }

    public static void test(int begin_id, int end_id) throws SQLException, ParseException {
        OHLC j,k;
        PinBar pb;
        OutSideBar ob;
        for (int i = begin_id; i <= end_id; i++) {
            j = getCandle("EURUSD_D1",i);
            k = getCandle("EURUSD_D1",i-1);
            pb = checkPB(j.o,j.h,j.l,j.c);
            ob = checkOB(j.h,j.l,j.o,j.c,k.h,k.l);
            if (pb.pb & check0(j.h,j.l)) System.out.println(j.date + " Pinbar trend " + pb.tr + " 0 " + check0(j.h,j.l) + " Trend " + checkTrend("EURUSD_D1",i,5,2));
            if (ob.ob & check0(j.h,j.l)) System.out.println(j.date + " Outsidebar trend " + ob.tr + " 0 " + check0(j.h,j.l) +" Trend " + checkTrend("EURUSD_D1",i,5,2));
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
            insert_db("GBPUSD240.csv","GBPUSD_H4");
        } catch (IOException e) {
            e.printStackTrace();
        }*/

        /*Date ll = new Date(1158159600000l);
        System.out.println(ll);*/

        //OHLC n = getCandle("EURUSD_D1","2020.01.03,00:00",true);
        //checkPB(n.o,n.h,n.l,n.c);
        //System.out.println(checkTrend("EURUSD_D1",11030,10,2));
        //System.out.println(check0(1.23934,1.22896));
        test(10556,11146);

        disconnect();


    }
}
