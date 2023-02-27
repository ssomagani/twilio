file -inlinebatch DROPS

drop procedure NewEvent IF EXISTS;
drop procedure session_rank IF EXISTS;
drop procedure session_rank IF EXISTS;
drop procedure max_data_by_sim IF EXISTS;
drop procedure session_freq_modal IF EXISTS;


drop view AGG_SESSIONS_DISCRETE IF EXISTS;
drop view SESSION_FREQ IF EXISTS;
drop view data_by_sim IF EXISTS;
drop view data_by_network IF EXISTS;
drop view hourly_data_by_sim IF EXISTS;

drop stream dump_events IF EXISTS;
drop stream NOTIFICATIONS IF EXISTS;

drop table event IF EXISTS;
drop table known_locations IF EXISTS;
drop table states IF EXISTS;
drop table FIXED_DEVICES IF EXISTS;
drop table MOBILE_DEVICES IF EXISTS;

DROPS

-- Tables --------------------------

file -inlinebatch TABLES

create table event (
	type varchar (64),
	id varchar (64) not null,
	time Timestamp,
	network varchar,
	data_download integer,
	data_session_update_end_time Timestamp,
	loc_long decimal,
	loc_lat decimal,
	cell_id varchar,
	lac varchar,
	tstamp Timestamp,
	data_session_update_start_time Timestamp,
	sim_unique_name varchar not null,
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
partition table event on column sim_unique_name;

create table known_locations (
 	name varchar (32) not null,
    coords geography_point not null
);

create table states (
	state varchar(32) not null,
	boundary GEOGRAPHY not null
);

create table fixed_devices (
	sim_unique_name varchar not null,
	home_loc varchar(32) not null,
	radius integer not null,
	data_download_limit integer not null,
	data_upload_limit integer not null,
	lac_threshold integer not null,
	registered_imei varchar
);

create table mobile_devices (
	sim_unique_name varchar not null,
	home_loc varchar(32) not null,
	data_download_limit integer not null,
	data_upload_limit integer not null,
	lac_threshold integer not null,
	registered_imei varchar
);

-- Streams --------------------------

create stream dump_events partition on column sim_unique_name export to target dump_events (
	type varchar (64),
	id varchar (64) not null,
	time Timestamp,
	network varchar,
	data_download integer,
	data_session_update_end_time Timestamp,
	loc_long decimal,
	loc_lat decimal,
	cell_id varchar,
	lac varchar,
	tstamp Timestamp,
	data_session_update_start_time Timestamp,
	sim_unique_name varchar not null,
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

create stream notifications partition on column sim_unique_name export to target notifications (
	sim_unique_name varchar not null,
	time Timestamp,
	message varchar not null
); 

-- Streams --------------------------

CREATE VIEW agg_sessions_discrete 
as 
select 
	network, sim_unique_name, rat_type, ip_address, 
	apn, imsi, count(*) sessions 
	from event 
	group by network, sim_unique_name, rat_type, ip_address, apn, imsi;

CREATE VIEW data_by_sim
as 
select 
	HOUR(TIME_WINDOW(HOUR, 1, TIME)) as intervl, 
	sim_unique_name, 
	sum(data_session_data_total) as data 
	from 
	event group by intervl, sim_unique_name;
	
CREATE VIEW hourly_data_by_sim 
as 
select 
	HOUR(TIME_WINDOW(HOUR, 1, TIME)) as intervl, 
	truncate(DAY, TIME) as day,
	sim_unique_name, 
	sum(data_session_data_total) as data 
	from 
	event group by intervl, day, sim_unique_name;

CREATE VIEW data_by_network as select network from event group by network;

CREATE VIEW session_freq as
	select 
	distinct(DATEDIFF(SECOND, data_session_update_start_time, data_session_update_end_time)) as session_duration, 
	sim_unique_name, 
	count(*) as freq 
	from event 
	group by session_duration, sim_unique_name;
	
TABLES

------	
load classes classes.jar;
LOAD CLASSES model.jar;
------

file -inlinebatch PROCS

create function dat_5_model FROM METHOD Dat_5_model.dat_5_model;

create procedure partition on table event column sim_unique_name parameter 12 from class com.voltdb.examples.NewEvent;
	
create procedure max_hourly_data_by_sim as select max(data) from hourly_data_by_sim group by sim_unique_name;
	
create procedure session_freq_modal as 
	select session_duration/10 intervl, sum(freq) sum_freq from session_freq group by intervl order by sum_freq; 
	
create procedure session_rank as 
	select 
	rank() over (partition by sim_unique_name order by data desc) as rnk, 
	sim_unique_name, 
	intervl, 
	data 
	from data_by_sim order by rnk, sim_unique_name;
	
create procedure session_duration_rank PARTITION ON TABLE A COLUMN SIM_UNIQUE_NAME as 
	select rnk from 
	(select 
		rank() over (partition by sim_unique_name order by freq desc) as rnk, 
		sim_unique_name, freq, session_duration from session_freq) a 
		where sim_unique_name=? and session_duration=?;
	
PROCS