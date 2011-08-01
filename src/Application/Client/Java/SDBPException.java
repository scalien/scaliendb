package com.scalien.scaliendb;

public class SDBPException extends java.lang.Exception {
    private int status;

    public SDBPException(int status) {
        super(Status.toString(status) + " (code " + status + ")");
        this.status = status;
    }

    public SDBPException(int status, String txt) {
        super(Status.toString(status) + " (code " + status + "): " + txt);
        this.status = status;
    }
    
    public int getStatus() {
        return status;
    }
}
