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
		Object[] procArgs = new Object[27];

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

			HashMap<String, Object> loc = (HashMap<String, Object>) data.get("location");
			Double lon = (Double) loc.get("lon");
			Double lat = (Double) loc.get("lat");
			String cellId = (String) loc.get("cell_id");
			GeographyPointValue latLong = new GeographyPointValue(lon, lat);

			procArgs[0] = map.get("type");
			procArgs[1] = map.get("id");
			procArgs[2] = tim;
			procArgs[3] = network.get("friendly_name");
			procArgs[4] = data.get("data_download");
			procArgs[5] = dataSessionUpdateEndTime;
			procArgs[6] = latLong;
			procArgs[7] = tstamp;
			procArgs[8] = dataSessionUpdateStartTime;
			procArgs[9] = data.get("sim_unique_name");
			procArgs[10] = data.get("imei");
			procArgs[11] = data.get("data_session_sid");
			procArgs[12] = data.get("data_session_data_total");
			procArgs[13] = dataSessionStartTime;
			procArgs[14] = data.get("event_sid");
			procArgs[15] = data.get("fleet_sid");
			procArgs[16] = data.get("rat_type");
			procArgs[17] = data.get("data_session_data_download");
			procArgs[18] = data.get("data_total");
			procArgs[19] = data.get("data_upload");
			procArgs[20] = data.get("ip_address");
			procArgs[21] = data.get("apn");
			procArgs[22] = data.get("sim_sid");
			procArgs[23] = data.get("account_sid");
			procArgs[24] = data.get("sim_iccid");
			procArgs[25] = data.get("imsi");
			procArgs[26] = data.get("data_session_data_upload");
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
		} catch (ParseException e) {
			date = sdf2.parse(text);
		} 
		return outputSDF.format(date);
	}
}
