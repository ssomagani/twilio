package com.voltdb.examples;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.TimestampType;

import com.fasterxml.jackson.core.exc.StreamReadException;
import com.fasterxml.jackson.databind.DatabindException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class TestFormatter {

	public static void main(String[] args) throws StreamReadException, DatabindException, IOException, ParseException {
		String buffer = "{\"specversion\":\"1.0\",\"type\":\"com.twilio.iot.supersim.connection.data-session.updated\",\"source\":\"/v1/SuperSim/ConnectionEvents/EZeb42ec296e094786c6dd34aab7b15fac\",\"id\":\"EZeb42ec296e094786c6dd34aab7b15fac\",\"dataschema\":\"https://events-schemas.twilio.com/SuperSim.ConnectionEvent/2\",\"datacontenttype\":\"application/json\",\"time\":\"2022-12-14T13:40:52.000Z\",\"data\":{\"network\":{\"iso_country\":\"US\",\"mnc\":\"260\",\"mcc\":\"310\",\"sid\":\"HWd46933be78a54c05b5c287ddeb9592f4\",\"friendly_name\":\"T-Mobile\"},\"data_download\":0,\"data_session_update_end_time\":\"2022-12-14T13:40:52Z\",\"location\":{\"lon\":-118.5720773,\"cell_id\":\"47322374\",\"lat\":34.166991,\"lac\":\"15170\"},\"timestamp\":\"2022-12-14T13:40:52Z\",\"data_session_update_start_time\":\"2022-12-14T13:34:52Z\",\"sim_unique_name\":\"Zeblaze Thor 6 Android Watch\",\"imei\":\"358600842867678\",\"data_session_sid\":\"PIc64fc7780f12ab694aad316d7b06f7a3\",\"data_session_data_total\":0,\"data_session_start_time\":\"2022-12-11T06:15:01Z\",\"event_sid\":\"EZeb42ec296e094786c6dd34aab7b15fac\",\"fleet_sid\":\"HF925ae5b4af6aeb7d5034d3f4b09c78ce\",\"rat_type\":\"4G LTE\",\"data_session_data_download\":0,\"data_total\":0,\"data_upload\":0,\"event_type\":\"com.twilio.iot.supersim.connection.data-session.updated\",\"ip_address\":\"100.68.56.159\",\"apn\":\"super\",\"sim_sid\":\"HS2f71995b834d803ff5376799ba59bd1d\",\"account_sid\":\"ACe7ea554f7fc0fad65e689bb5899bb6c3\",\"sim_iccid\":\"89883070000005381783\",\"imsi\":\"732123200563187\",\"data_session_data_upload\":0}}";

		ObjectMapper objectMapper = new ObjectMapper();

		HashMap<String, Object> map = objectMapper.readValue(buffer, HashMap.class);
		HashMap<String, Object> data = (HashMap) map.get("data");
		HashMap<String, HashMap> network = (HashMap) data.get("network");
		TimestampType tim = getTimestamp((String) map.get("time"));
		TimestampType tstamp = getTimestamp((String) data.get("timestamp"));

		HashMap<String, Object> loc = (HashMap<String, Object>) data.get("location");
		Double lon = (Double) loc.get("lon");
		Double lat = (Double) loc.get("lat");
		String cellId = (String) loc.get("cell_id");
		GeographyPointValue latLong = new GeographyPointValue(lon, lat);

		Object[] procArgs = new Object[27];
		procArgs[0] = map.get("type");
		procArgs[1] = map.get("id");
		procArgs[2] = tim;
		procArgs[3] = network.get("friendly_name");
		procArgs[4] = data.get("data_download");
		procArgs[5] = data.get("data_session_update_end_time");
		procArgs[6] = latLong;
		procArgs[7] = tstamp;
		procArgs[8] = data.get("data_session_update_start_time");
		procArgs[9] = data.get("sim_unique_name");
		procArgs[10] = data.get("imei");
		procArgs[11] = data.get("data_session_sid");
		procArgs[12] = data.get("data_session_data_total");
		procArgs[13] = data.get("data_session_start_time");
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

		System.out.println(data);
		System.out.println(map.get("type"));
		System.out.println(map.get("id"));
		System.out.println(tim);

		System.out.println(network.get("friendly_name"));
		System.out.println(data.get("data_download"));
		System.out.println(data.get("data_session_update_end_time"));
		System.out.println(lon + ":" + lat);
		System.out.println(cellId);

		System.out.println(data.get("timestamp"));
		System.out.println(data.get("data_session_update_start_time"));
		System.out.println(data.get("sim_unique_name"));
		System.out.println(data.get("imei"));
		System.out.println(data.get("data_session_sid"));
	}

	private static TimestampType getTimestamp(String value) throws ParseException {
		Date date = getDate(value);
		TimestampType tst = new TimestampType(date);
		return tst;
	}

	private static Date getDate(String text) throws ParseException {
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
		Date date;
		try {
			date = sdf1.parse(text);
		} catch (ParseException e) {
			date = sdf2.parse(text);
		} 
		return date;
	}
}
