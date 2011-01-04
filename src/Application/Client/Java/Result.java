package com.scalien.scaliendb;

import java.math.BigInteger;
import java.util.TreeMap;

public class Result
{
	private SWIGTYPE_p_void cptr;
	
	Result(SWIGTYPE_p_void cptr) {
		this.cptr = cptr;
	}
	
	protected void finalize() {
        //System.out.println("finalizing Result");
		scaliendb_client.SDBP_ResultClose(cptr);
	}
	
    /**
     * Returns the current key in result.
     */
	public String getKey() {
		return scaliendb_client.SDBP_ResultKey(cptr);
	}
	
    /**
     * Returns the current value in result.
     */
	public String getValue() {
		return scaliendb_client.SDBP_ResultValue(cptr);
	}
    
    /**
     * Returns the current key in result.
     */
    public byte[] getKeyBytes() {
        return scaliendb_client.SDBP_ResultKeyBuffer(cptr);
    }
	
    /**
     * Returns the current value in result.
     */
    public byte[] getValueBytes() {
        return scaliendb_client.SDBP_ResultValueBuffer(cptr);
    }
	
    /**
     * Returns the current numeric value in result.
     */
    public long getNumber() {
        BigInteger bi = scaliendb_client.SDBP_ResultNumber(cptr);
        return bi.longValue();
    }
    
    /**
     * Jumps to the first elem in the result.
     */
	public void begin() {
		scaliendb_client.SDBP_ResultBegin(cptr);
	}
	
    /**
     * Moves to the next elem in the result.
     */
	public void next() {
		scaliendb_client.SDBP_ResultNext(cptr);
	}
	
    /**
     * Returns true, if the current elem is the last one.
     */
	public boolean isEnd() {
		return scaliendb_client.SDBP_ResultIsEnd(cptr);
	}
	
    /**
     * Returns the transport status.
     */
	public int getTransportStatus() {
		return scaliendb_client.SDBP_ResultTransportStatus(cptr);
	}
	
    /**
     * Returns the command status.
     */
	public int getCommandStatus() {
		return scaliendb_client.SDBP_ResultCommandStatus(cptr);
	}	
	
    /**
     * Returns the key-value mappings in a TreeMap.
     */
	public TreeMap<String, String> getKeyValues() {
		TreeMap<String, String> keyvals = new TreeMap<String, String>();
		for (begin(); !isEnd(); next())
			keyvals.put(getKey(), getValue());
			
		return keyvals;
	}
}
