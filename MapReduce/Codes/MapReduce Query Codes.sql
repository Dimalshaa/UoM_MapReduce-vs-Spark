DROP TABLE IF EXISTS AirLineDelay;
DROP TABLE IF EXISTS summaryDelay;
DROP TABLE IF EXISTS eachDelay;

CREATE EXTERNAL TABLE AirLineDelay 
(rowID bigint, Year int, Month int, DayofMonth int, DayOfWeek int, 
DepTime double, CRSDepTime int, ArrTime double, CRSArrTime double, 
UniqueCarrier string, FlightNum int, TailNum String, ActualElapsedTime double, 
CRSElapsedTime double, AirTime double, ArrDelay double, DepDelay double, 
Origin String, Dest String, Distance int, TaxiIn double, TaxiOut double, 
Cancelled int, CancellationCode String, Diverted int, CarrierDelay double, 
WeatherDelay double, NASDelay double, SecurityDelay double, LateAircraftDelay double) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
STORED AS TEXTFILE 
LOCATION "s3://thathsara-video-presentation-assignment/input/";

CREATE EXTERNAL TABLE summaryDelay(
Year string,
carrier_delay string,
nas_delay string,
weather_delay string,
late_aircraft_delay string,
security_delay string)
STORED AS SEQUENCEFILE
LOCATION "s3://thathsara-video-presentation-assignment/tables/";

-- Agaist Year Summary delay from 2003-2010
INSERT OVERWRITE TABLE summaryDelay
SELECT Year, SUM(CarrierDelay) AS carrier_delay, 
SUM(NASDelay) AS nas_delay,
SUM(WeatherDelay) AS weather_delay,
SUM(LateAircraftDelay) AS late_aircraft_delay,
SUM(SecurityDelay) AS security_delay
FROM AirLineDelay
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

CREATE EXTERNAL TABLE eachDelay(
Year string,
delay string)
STORED AS SEQUENCEFILE
LOCATION "s3://thathsara-video-presentation-assignment/tables/";

-- Year wise carrier delay from 2003-2010
INSERT OVERWRITE TABLE eachDelay
SELECT Year, SUM(CarrierDelay) AS carrier_delay 
FROM AirLineDelay
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Year wise NAS delay from 2003-2010
INSERT OVERWRITE TABLE eachDelay
SELECT Year,SUM(NASDelay) AS nas_delay
FROM AirLineDelay
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Year wise Weather delay from 2003-2010
INSERT OVERWRITE TABLE eachDelay
SELECT Year, SUM(WeatherDelay) AS weather_delay
FROM AirLineDelay
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Year wise late aircraft delay from 2003-2010
INSERT OVERWRITE TABLE eachDelay
SELECT Year, SUM(LateAircraftDelay) AS late_aircraft_delay
FROM AirLineDelay
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;

-- Year wise security delay from 2003-2010
INSERT OVERWRITE TABLE eachDelay
SELECT Year, SUM(SecurityDelay) AS security_delay
FROM AirLineDelay
WHERE Year >= 2003 AND Year <= 2010
GROUP BY Year;
