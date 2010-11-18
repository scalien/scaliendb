package com.scalien.scaliendb;

import java.math.BigInteger;
import java.util.TreeMap;

public class Result
{
	private SWIGTYPE_p_void cptr;
	
	public Result(SWIGTYPE_p_void cptr) {
		this.cptr = cptr;
	}
	
	public void finalize() {
		scaliendb_client.SDBP_ResultClose(cptr);
	}
	
	public String getKey() {
		return scaliendb_client.SDBP_ResultKey(cptr);
	}
	
	public String getValue() {
		return scaliendb_client.SDBP_ResultValue(cptr);
	}
    
    public byte[] getKeyBytes() {
        return scaliendb_client.SDBP_ResultKeyBuffer(cptr);
    }
	
    public byte[] getValueBytes() {
        return scaliendb_client.SDBP_ResultValueBuffer(cptr);
    }
	
    public long getNumber() {
        BigInteger bi = scaliendb_client.SDBP_ResultNumber(cptr);
        return bi.longValue();
    }
    
	public void begin() {
		scaliendb_client.SDBP_ResultBegin(cptr);
	}
	
	public void next() {
		scaliendb_client.SDBP_ResultNext(cptr);
	}
	
	public boolean isEnd() {
		return scaliendb_client.SDBP_ResultIsEnd(cptr);
	}
	
	public int getTransportStatus() {
		return scaliendb_client.SDBP_ResultTransportStatus(cptr);
	}
	
	public int getCommandStatus() {
		return scaliendb_client.SDBP_ResultCommandStatus(cptr);
	}	
	
	public TreeMap<String, String> getKeyValues() {
		TreeMap<String, String> keyvals = new TreeMap<String, String>();
		for (begin(); !isEnd(); next())
			keyvals.put(getKey(), getValue());
			
		return keyvals;
	}
}
