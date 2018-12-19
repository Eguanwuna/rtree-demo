package cn.analysys.tag.utils;

import cn.analysys.tag.entity.IPArea;
import cn.analysys.tag.entity.IPData;

import java.util.List;

public class IpUtil {

    static int COMPARE_SUCCESS = 0;

    public static String getGpsByIp(String ip) {
        try {
            long ipNum = ipToLong(ip);
            IPData ipData = InitStandardUtil.getIpData();
            IPArea ipArea = search(ipData.getIpList(), ipNum);
            if (null != ipArea) {
                String key = String.valueOf(ipArea.getStartIp()) + ";" + String.valueOf(ipArea.getEndIp());
                return ipData.getIpMap().get(key);
            }
//            System.out.println("经纬度信息为空......");
        } catch (Exception e) {
            e.printStackTrace();
            System.out.println("ip error: " + ip);
        }
        return null;
    }

    private static IPArea search(List<IPArea> list, Long ipLong) {
        if (list == null || ipLong == null) {
            return null;
        }
        IPArea sr = null;
        int iindex = 0; // 相当于指针的东西
        int istart = 0; //
        int iend = list.size() - 1;
        for (int i = 0; i < list.size(); i++) {
            if (list.size() <= 2) {
                iindex = i;
            } else {
                iindex = (istart + iend) / 2;
                if (iindex == istart && istart == (iend - 1) && iindex != 0) {
                    iindex++;
                }
            }
            sr = list.get(iindex);
            // 使用对象中的key进行查询
            int compare = ipLong.compareTo(sr.getStartIp());

            if (compare >= COMPARE_SUCCESS) {
                if (sr.getStartIp() <= ipLong && sr.getEndIp() >= ipLong) {
                    return sr;
                }
                istart = iindex;
            } else if (compare < COMPARE_SUCCESS) {
                iend = iindex;
            } else {
                // sis.setSex(sr.getSex());
                // sis.setMobile(sr.getMobile());
                break;
            }
            if (istart == iend) {
                break;
            }
        }
        return null;
    }

    private static long ipToLong(String ip) {
        String[] quads = ip.replaceAll(",", ".").split("\\.");
        /*原方法
        String[] ipParts = ip.split("\\.");
        long ipNum = (Long.parseLong(ipParts[0]) << 24) + (Long.parseLong(ipParts[1]) << 16) + (Long.parseLong(ipParts[2]) << 8) + Long.parseLong(ipParts[3]);*/
        long result = 0;
        try {
            result += Integer.parseInt(quads[3]);
            result += Long.parseLong(quads[2]) << 8L;
            result += Long.parseLong(quads[1]) << 16L;
            result += Long.parseLong(quads[0]) << 24L;
        } catch (Exception e) {
            System.out.println("ip " + ip + ",formatted error!");
        }
        return result;
    }

    public static void main(String[] args) {
        for (int i = 0; i < 100; i++) {
            long time = System.currentTimeMillis();
            String ip = "192.168.220.149";
            String gpsByIp = getGpsByIp(ip);
            System.out.println(System.currentTimeMillis() - time);
        }
    }
}
