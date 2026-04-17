package ie.rolfe.s2.chargingdemo;

class TxRequest {
    public int randomuser;
    public long pid;
    public long txId;
    public long createMS = System.currentTimeMillis();

    public TxRequest(long txId, long pid, int randomuser) {
        this.txId = txId;
        this.pid = pid;
        this.randomuser = randomuser;
    }
}
