package com.voltdb.formatters;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;

import org.voltdb.importer.formatter.FormatException;
import org.voltdb.importer.formatter.Formatter;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.TimestampType;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonFormatter implements Formatter {

	@Override
	public Object[] transform(ByteBuffer payload) throws FormatException {

		String buffer = new String(payload.array());
		Object[] procArgs = new Object[31];

		try {
			ObjectMapper objectMapper = new ObjectMapper();

			HashMap<String, Object> map = objectMapper.readValue(buffer, HashMap.class);
			HashMap<String, Object> data = (HashMap) map.get("data");
			HashMap<String, HashMap> network = (HashMap) data.get("network");
			TimestampType tim = getTimestamp((String) map.get("time"));
			TimestampType tstamp = getTimestamp((String) data.get("timestamp"));
			TimestampType dataSessionUpdateStartTime = getTimestamp((String) data.get("data_session_update_start_time"));
			TimestampType dataSessionUpdateEndTime = getTimestamp((String) data.get("data_session_update_end_time"));
			TimestampType dataSessionStartTime = getTimestamp((String) data.get("data_session_start_time"));
			TimestampType dataSessionEndTime = getTimestamp((String) data.get("data_session_end_time"));

			HashMap<String, Object> loc = (HashMap<String, Object>) data.get("location");
			Double lon = (Double) loc.get("lon");
			Double lat = (Double) loc.get("lat");
			String cellId = (String) loc.get("cell_id");
			String lac = (String) loc.get("lac");

			procArgs[0] = map.get("type");
			procArgs[1] = map.get("id");
			procArgs[2] = tim;
			procArgs[3] = network.get("friendly_name");
			procArgs[4] = data.get("data_download");
			procArgs[5] = dataSessionUpdateEndTime;
			procArgs[6] = lon;
			procArgs[7] = lat;
			procArgs[8] = cellId;
			procArgs[9] = lac;
			procArgs[10] = tstamp;
			procArgs[11] = dataSessionUpdateStartTime;
			procArgs[12] = data.get("sim_unique_name");
			procArgs[13] = data.get("imei");
			procArgs[14] = data.get("data_session_sid");
			procArgs[15] = data.get("data_session_data_total");
			procArgs[16] = dataSessionStartTime;
			procArgs[17] = data.get("event_sid");
			procArgs[18] = data.get("fleet_sid");
			procArgs[19] = data.get("rat_type");
			procArgs[20] = data.get("data_session_data_download");
			procArgs[21] = data.get("data_total");
			procArgs[22] = data.get("data_upload");
			procArgs[23] = data.get("ip_address");
			procArgs[24] = data.get("apn");
			procArgs[25] = data.get("sim_sid");
			procArgs[26] = data.get("account_sid");
			procArgs[27] = data.get("sim_iccid");
			procArgs[28] = data.get("imsi");
			procArgs[29] = data.get("data_session_data_upload");
			procArgs[30] = dataSessionEndTime;
		} catch (IOException | ParseException e) {
			e.printStackTrace();
		}

		return procArgs;
	}
	private static TimestampType getTimestamp(String value) throws ParseException {
		String dateString = cleanUpDate(value);
		TimestampType tst = new TimestampType(dateString);
		return tst;
	}

	private static String cleanUpDate(String text) throws ParseException {
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		SimpleDateFormat outputSDF = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");
		Date date;
		try {
			date = sdf1.parse(text);
		} catch (Exception e) {
			try {
				date = sdf2.parse(text);
			} catch (Exception e1) {
				System.out.println(text);
				return "";
			}
		} 
		return outputSDF.format(date);
	}
}
