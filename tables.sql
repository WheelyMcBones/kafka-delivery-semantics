create database test;
use test;
create table msg_data(skey varchar(50), svalue varchar(50));
create table msg_offsets(topic_name varchar(50), partition_num int, offset_num int);
insert into msg_offsets values('SensorTopic',0,0);
insert into msg_offsets values('SensorTopic',1,0);
insert into msg_offsets values('SensorTopic',2,0);
