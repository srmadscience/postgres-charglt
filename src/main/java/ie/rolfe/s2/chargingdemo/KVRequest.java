package ie.rolfe.s2.chargingdemo;

import com.google.gson.Gson;

import java.util.Random;

public class KVRequest {
    public long createMS = System.currentTimeMillis();

    int userCount;
    int jsonsize;
    int deltaProportion;
    UserKVState[] userState;
    int oursession;
    long startMs;
    Random r;
    Gson gson;


    public KVRequest(int userCount, int jsonsize, int deltaProportion, UserKVState[] userState, int oursession, long startMs, Random r, Gson gson) {
        this.createMS = System.currentTimeMillis();
        this.userCount = userCount;
        this.jsonsize = jsonsize;
        this.deltaProportion = deltaProportion;
        this.userState = userState;
        this.oursession = oursession;
        this.startMs = startMs;
        this.r = r;
        this.gson = gson;
    }
}
