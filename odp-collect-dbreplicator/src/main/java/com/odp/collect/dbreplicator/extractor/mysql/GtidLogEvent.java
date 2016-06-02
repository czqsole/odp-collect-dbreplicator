package com.odp.collect.dbreplicator.extractor.mysql;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.continuent.tungsten.replicator.ReplicatorException;
import com.continuent.tungsten.replicator.extractor.mysql.conversion.LittleEndianConversion;

public class GtidLogEvent extends LogEvent{
	private Logger logger = Logger.getLogger(GtidLogEvent.class);
	
	private String gtid = null;
	
	private FormatDescriptionLogEvent descriptionEvent = null;

	public GtidLogEvent(byte[] buffer, int eventLength,
            FormatDescriptionLogEvent descriptionEvent, boolean useBytesForString,
            String currentPosition) throws ReplicatorException, IOException 
	{
		//super(buffer, descriptionEvent, useBytesForString);
		super(buffer, descriptionEvent, MysqlBinlog.GTID_LOG_EVENT);
        this.descriptionEvent  = descriptionEvent;
        int offset = 19;
        int flags = LittleEndianConversion.convert1ByteToInt(buffer,0);
        offset++;
        
        byte[] sid = new byte[16];
        System.arraycopy(buffer, offset, sid, 0, 16);
        offset += 16;
        
        long gno = LittleEndianConversion.convert8BytesToLong(buffer, offset);
        
        String gtid = new String(byteArrayToHex(sid, 0, 4) + "-" +
                byteArrayToHex(sid, 4, 2) + "-" +
                byteArrayToHex(sid, 6, 2) + "-" +
                byteArrayToHex(sid, 8, 2) + "-" +
                byteArrayToHex(sid, 10, 6) + ":" +
                String.format("%d", gno));
        logger.info("gtid:" + gtid);
        this.gtid = gtid;
	}
	
	 private String byteArrayToHex(byte[] a, int offset, int len) {
	        StringBuilder sb = new StringBuilder();
	        for (int idx = offset; idx < (offset + len) && idx < a.length; idx++) {
	            sb.append(String.format("%02x", a[idx] & 0xff));
	        }
	        return sb.toString();
	    }

	public String getGtid() {
		return gtid;
	}

	public void setGtid(String gtid) {
		this.gtid = gtid;
	}
}
