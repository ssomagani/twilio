create table event (
	type varchar (64),
	id varchar (64) not null,
	time Timestamp,
	network varchar,
	data_download integer,
	data_session_update_end_time Timestamp,
	loc GEOGRAPHY_POINT,
	tstamp Timestamp,
	data_session_update_start_time Timestamp,
	sim_unique_name varchar,
	imei varchar,
	data_session_sid varchar,
	data_session_data_total bigint,
	data_session_start_time Timestamp,
	event_sid varchar,
	fleet_sid varchar,
	rat_type varchar,
	data_session_data_download bigint,
	data_total integer,
	data_upload integer,
	ip_address varchar (32),
	apn varchar,
	sim_sid varchar,
	account_sid varchar,
	sim_iccid varchar,
	imsi varchar,
	data_session_data_upload bigint
);
partition table event on column id;

create table locations (
	loc GEOGRAPHY_POINT,
	city varchar (32),
	state varchar (2)
);

load classes classes.jar;

create procedure partition on table event column id from class com.voltdb.examples.NewEvent;

create view data_by_sim as select sum(data_download), sim_unique_name from event group by sim_unique_name;
create view data_by_network as select sum(data_download), network from event group by network;

select 
	distinct(DATEDIFF(SECOND, data_session_update_start_time, data_session_update_end_time)) as session_duration, 
	sim_unique_name , 
	count(*) as freq 
	from event 
	group by session_duration, sim_unique_name order by freq desc;
