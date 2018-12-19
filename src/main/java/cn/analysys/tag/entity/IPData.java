package cn.analysys.tag.entity;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;

public class IPData implements Serializable {

    private HashMap<String, String> ipMap;
    private ArrayList<IPArea> ipList;

    public IPData(HashMap<String, String> ipMap, ArrayList<IPArea> ipList) {
        this.ipMap = ipMap;
        this.ipList = ipList;
    }

    public HashMap<String, String> getIpMap() {
        return ipMap;
    }

    public void setIpMap(HashMap<String, String> ipMap) {
        this.ipMap = ipMap;
    }

    public ArrayList<IPArea> getIpList() {
        return ipList;
    }

    public void setIpList(ArrayList<IPArea> ipList) {
        this.ipList = ipList;
    }
}
