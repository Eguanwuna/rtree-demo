package cn.analysys.tag.utils;

import cn.analysys.tag.entity.IPArea;
import cn.analysys.tag.entity.IPData;

import java.net.UnknownHostException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;

public class InitStandardUtil {

    private static IPData ipdata = null;
    private static boolean initState = false;

    private synchronized static void init() {
        if (!initState) {
            try {
                int retryNum = 10;
                dimProcess(retryNum);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException("dimProcess init error: " + e.toString());
            }
        }
        initState = true;
    }

    private static void dimProcess(int retryTime) throws Exception {
        if (retryTime == 0)
            throw new Exception("after 10 retryTimes,read database error!");
        else {
            try {
                ipdata = getIpDataSYN();
            } catch (Exception e) {
                e.printStackTrace();
                retryTime = retryTime - 1;
                dimProcess(retryTime);
            }
        }
    }

    /*public static HashMap<String, String> getIpMap() {
        if (ipMap == null) init();
        return ipMap;
    }*/

    public static IPData getIpData() {
        if (null == ipdata) init();
        return ipdata;
    }

    private static IPData getIpDataSYN() throws UnknownHostException {
        HashMap<String, String> ipMap = new HashMap<>();
        ArrayList<IPArea> ipListGet = new ArrayList<>();
        String ip_start, ip_end;
        String sql = "select ip_start_num, ip_end_num, longitude, latitude from `dim_base_ip` where longitude <> 229183 and longitude != '' and latitude != '' and longitude is not null and latitude is not null  order by ip_start_num";
        try {
            ResultSet rs = MysqlJDBCUtil.getResultSet(sql);
            while (rs.next()) {
                ip_start = rs.getString(1);
                ip_end = rs.getString(2);
                ipMap.put(ip_start + ";" + ip_end, rs.getString(3) + ";" + rs.getString(4).replace("\r", ""));
                ipListGet.add(new IPArea(Long.parseLong(ip_start), Long.parseLong(ip_end)));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(" getIpMapSYN ERROR! ");
        }
        return new IPData(ipMap, ipListGet);
    }

    /*IP段信息
    public static List<IPArea> getListIp() {
        if (ipList == null) {
            init();
        }
        return ipList;
    }

    private synchronized static List<IPArea> getListIpSYN() {
        String sql = "select ip_start_num,ip_end_num from dim_base_ip  where longitude is not null and latitude is not null and longitude <> '229183' and longitude <> ''  and latitude <> '' order by ip_start_num";
        List<IPArea> ipListGet = new ArrayList<>();
        //加快调试速度 可以注掉
        long ipStartNum = 0l;
        long ipEndNum = 0l;
        try {
            ResultSet rs = MysqlJDBCUtil.getResultSet(sql);
            while (rs.next()) {
                ipStartNum = rs.getLong(1);
                ipEndNum = rs.getLong(2);
                ipListGet.add(new IPArea(ipStartNum, ipEndNum));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(" getListIpSYN ERROR! ");
        }
        return ipListGet;
    }*/

}
