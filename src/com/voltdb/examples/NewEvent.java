package com.voltdb.examples;

import java.text.ParseException;

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
			+ "?, ?, ?, ?, ?"
			+ " )");

	private final SQLStmt GET_MOBILE_DEVICE_PROFILE = new SQLStmt("select * from mobile_devices where sim_unique_name = ?");
	private final SQLStmt GET_FIXED_DEVICE_PROFILE = new SQLStmt("select * from fixed_devices where sim_unique_name = ?");
	private final SQLStmt INSERT_NOTIF = new SQLStmt("insert into notifications values (?, ?, ?)");
	private final SQLStmt GET_IMEI = new SQLStmt("select imei from event where sim_unique_name = ?");
	//	private final SQLStmt CHECK_STATE = new SQLStmt("select state from states where CONTAINS( boundary ,POINTFROMTEXT(CONCAT('POINT(',CAST(? AS VARCHAR),' ',CAST(? AS VARCHAR),')') ))");
	private final SQLStmt CHECK_STATE = new SQLStmt("select state from states where CONTAINS( boundary, ?)");
	private final SQLStmt CHECK_DIST_FROM_HOME = new SQLStmt("select distance (POINTFROMTEXT(CONCAT('POINT(',CAST(? AS VARCHAR),' ',CAST(? AS VARCHAR),')') ), b.coords) from fixed_devices a, known_locations b where a.sim_unique_name=? and a.home_loc = b.name; ");
	private final SQLStmt GET_FIXED_LAC_ANOMALY = new SQLStmt("select dat_5_model (?, ?) from fixed_devices where sim_unique_name = ?");
	private final SQLStmt GET_MOBILE_LAC_ANOMALY = new SQLStmt("select dat_5_model (?, ?) from mobile_devices where sim_unique_name = ?");
	private final SQLStmt COMPARE_SESSION_FREQ = new SQLStmt("select max(session_duration) from session_freq where sim_unique_name = ?");

	private final SQLStmt DUMP_EVENTS = new SQLStmt("insert into dump_events values ("
			+ "?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?,"
			+ "?, ?, ?, ?, ?"
			+ " )");

	public VoltTable[] run(
			String type, 
			String id, 
			TimestampType time, 
			String network, 
			String data_download, 
			TimestampType data_session_update_end_time, 
			Double lon,
			Double lat,
			String cellId,
			String lac,
			TimestampType tstamp, 
			TimestampType data_session_update_start_time, 
			String sim_unique_name,
			String imei, 
			String data_session_sid, 
			String data_session_data_total, 
			TimestampType data_session_start_time, 
			String event_sid,
			String fleet_sid, 
			String rat_type, 
			String data_session_data_download, 
			String data_total, 
			String data_upload, 
			String ip_address, 
			String apn, 
			String sim_sid, 
			String account_sid, 
			String sim_iccid, 
			String imsi,
			String data_session_data_upload) 
					throws ParseException {

		String homeLoc = null;
		long radius = 50000;
		Boolean mobile = Boolean.FALSE;
		long data_download_limit = 5000;
		long data_upload_limit = 2000;
		long lac_threshold = 80;

		// Fetch Device Profile
		voltQueueSQL(GET_FIXED_DEVICE_PROFILE, sim_unique_name);
		VoltTable deviceTable = voltExecuteSQL()[0];
		if(deviceTable.advanceRow()) {
			homeLoc = deviceTable.getString(1);
			radius = deviceTable.getLong(2);
			data_download_limit = deviceTable.getLong(3);
			data_upload_limit = deviceTable.getLong(4);
			lac_threshold = deviceTable.getLong(5);
		} else {
			voltQueueSQL(GET_MOBILE_DEVICE_PROFILE, sim_unique_name);
			deviceTable = voltExecuteSQL()[0];
			if(deviceTable.advanceRow()) {
				mobile = Boolean.TRUE;
				homeLoc = deviceTable.getString(1);
				data_download_limit = deviceTable.getLong(2);
				data_upload_limit = deviceTable.getLong(3);
				lac_threshold = deviceTable.getLong(4);
			} else {
				voltQueueSQL(INSERT_NOTIF, sim_unique_name, time, "Unknown device detected");
				voltExecuteSQL();
			}
		}

		// Evaluate data Rules
		if(data_download_limit < Integer.parseInt(data_download)) {
			voltQueueSQL(INSERT_NOTIF, sim_unique_name, time, "Data download over limit " + data_download);
			voltExecuteSQL();
		}
		if(data_upload_limit < Integer.parseInt(data_upload)) {
			voltQueueSQL(INSERT_NOTIF, sim_unique_name, time, "Data upload over limit " + data_upload);
			voltExecuteSQL();
		}

		// Evaluate Session Duration Rule
		voltQueueSQL(COMPARE_SESSION_FREQ, sim_unique_name);
		VoltTable sessionFreqTable = voltExecuteSQL()[0];
		if(sessionFreqTable.advanceRow()) {
			long sessionDurationMax = sessionFreqTable.getLong(0);
			long sessionDuration = (data_session_update_end_time.getTime() - data_session_update_start_time.getTime())/1000000;
			if(sessionDuration > sessionDurationMax) {
				voltQueueSQL(INSERT_NOTIF, sim_unique_name, time, "Anomalous session duration detected " + sessionDuration);
				voltExecuteSQL();
			}
		}

		// Evaluate IMEI Rules
		voltQueueSQL(GET_IMEI, sim_unique_name);
		VoltTable imeiTable = voltExecuteSQL()[0];
		if(imeiTable.advanceRow()) {
			String prevImei = imeiTable.getString(0);
			if(!prevImei.equals(imei)) {
				voltQueueSQL(INSERT_NOTIF, sim_unique_name, time, "IMEI change detected");
				voltExecuteSQL();
			}
		}

		// Evaluate Location Rules
		if(mobile) {
			GeographyPointValue gp = new GeographyPointValue(lon, lat);
			voltQueueSQL(CHECK_STATE, gp);
			VoltTable results[] = voltExecuteSQL();
			VoltTable stateTable = results[0];
			if(stateTable.advanceRow()) {
				String state = stateTable.getString(0);
				if(!state.equals(homeLoc)) {
					voltQueueSQL(INSERT_NOTIF, sim_unique_name, time, "Device Out of State in " + state + " when expected in " + homeLoc);
					voltExecuteSQL();
				}
			} else {
				voltQueueSQL(INSERT_NOTIF, sim_unique_name, time, "Out of State location detected");
				voltExecuteSQL();
			}
		} else {
			voltQueueSQL(CHECK_DIST_FROM_HOME, lon, lat, sim_unique_name);
			VoltTable results[] = voltExecuteSQL();
			VoltTable distanceTable = results[0];
			if(distanceTable.advanceRow()) {
				double distance = distanceTable.getDouble(0);
				if(distance > radius) {
					voltQueueSQL(INSERT_NOTIF, sim_unique_name, time, "Device straying from home " + homeLoc);
					voltExecuteSQL();
				}
			}
		}

		// Infer Location ML
		SQLStmt lacAnomalyProc = GET_FIXED_LAC_ANOMALY;
		if(mobile)
			lacAnomalyProc = GET_MOBILE_LAC_ANOMALY;
		try {
			voltQueueSQL(lacAnomalyProc, Integer.parseInt(lac), ip_address, sim_unique_name);
			VoltTable results[] = voltExecuteSQL();
			VoltTable anomalyTable = results[0];
			if(anomalyTable.advanceRow()) {
				long target = anomalyTable.getLong(0);
				if(target > lac_threshold) {
					voltQueueSQL(INSERT_NOTIF, sim_unique_name, time, "Anomalous location detected");
					voltExecuteSQL();
				}
			}
		} catch (Exception e) {

		}

		// Insert into event table
		voltQueueSQL(INSERT, type, id, time, network, data_download, 
				data_session_update_end_time, lon, lat, cellId, lac, tstamp,
				data_session_update_start_time, 
				sim_unique_name,
				imei, data_session_sid, data_session_data_total,
				data_session_start_time, event_sid, fleet_sid,
				rat_type, data_session_data_download, data_total,
				data_upload, ip_address, apn, sim_sid, account_sid,
				sim_iccid, imsi, data_session_data_upload);

		// Insert into dump stream
		voltQueueSQL(DUMP_EVENTS, type, id, time, network, data_download, 
				data_session_update_end_time, lon, lat, cellId, lac, tstamp,
				data_session_update_start_time, 
				sim_unique_name,
				imei, data_session_sid, data_session_data_total,
				data_session_start_time, event_sid, fleet_sid,
				rat_type, data_session_data_download, data_total,
				data_upload, ip_address, apn, sim_sid, account_sid,
				sim_iccid, imsi, data_session_data_upload);
		return voltExecuteSQL();
	}
}
