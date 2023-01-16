package com.voltdb.examples;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.TimestampType;

public class NewEvent extends VoltProcedure {

	private final SQLStmt INSERT = new SQLStmt("insert into event values ("
			+ "?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?,"
			+ "?, ?"
			+ " )");
	
	public VoltTable[] run(
			String type, 
			String id, 
			TimestampType time, 
			String network, 
			String data_download, 
			TimestampType data_session_update_end_time, 
			GeographyPointValue location, 
			TimestampType tstamp, 
			TimestampType data_session_update_start_time, String sim_unique_name,
			String imei, String data_session_sid, String data_session_data_total, 
			TimestampType data_session_start_time, String event_sid, String fleet_sid, 
			String rat_type, String data_session_data_download, String data_total, 
			String data_upload, String ip_address, 
			String apn, String sim_sid, String account_sid, 
			String sim_iccid, String imsi, String data_session_data_upload) 
					throws ParseException {
		
		
		voltQueueSQL(INSERT, type, id, time, network, data_download, 
				data_session_update_end_time, location, tstamp,
				data_session_update_start_time, sim_unique_name,
				imei, data_session_sid, data_session_data_total,
				data_session_start_time, event_sid, fleet_sid,
				rat_type, data_session_data_download, data_total,
				data_upload, ip_address, apn, sim_sid, account_sid,
				sim_iccid, imsi, data_session_data_upload);
		return voltExecuteSQL();
	}
}
