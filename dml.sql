insert into states values ('California', POLYGONFROMTEXT('POLYGON ((-124.356844 42.0164786,-121.1048908 32.1196036,-114.5350666 32.7686044,-114.1835041 34.2887992,-119.9623127 38.9592275,-120.006258 42.0001519,-124.356844 42.0164786))'));
insert into states values ('Kentucky', POLYGONFROMTEXT('POLYGON ((-89.4447508 36.5157008, -83.6659422 36.6039468, -82.0399656 37.489564, -82.589282 38.1664091, -82.8968992 38.7256917, -83.6769285 38.6485112, -84.5668211 39.0677078, -85.6544676 38.373418, -86.3466063 38.1232079, -88.0165281 37.9674722, -88.4230223 37.1838375, -89.0712156 37.1925898, -89.4447508 36.5157008))'));

insert into known_locations values ('Los Angeles', POINTFROMTEXT ('POINT (-118.4068 34.1139)'));
insert into known_locations values ('Lexington', POINTFROMTEXT ('POINT (84.5459 38.0610)'));
insert into known_locations values ('Paducah', POINTFROMTEXT ('POINT (-88.6234 37.0903)'));

insert into mobile_devices values ('GV500MAP - Ranger', 'Kentucky', 7500000, 7500000, 90, '862061048023666');
insert into fixed_devices values ('GL-X1200 SIM 1a (EP06)', 'Paducah', 20000, 7500000, 7500000, 80, '99');
insert into fixed_devices values ('Samsung Tab S5e', 'Paducah', 20000, 7500000, 7500000, 80, '358590101001806');
insert into fixed_devices values ('Zeblaze Thor 6 Android Watch', 'Los Angeles', 20000, 7500000, 7500000, 80, '358600842867678');
