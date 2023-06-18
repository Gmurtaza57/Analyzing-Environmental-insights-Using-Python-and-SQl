
-- 1 Average temperature recorded for each device.

SELECT device_id, AVG(temperature) AS average_temperature
FROM cleaned_environment
GROUP BY device_id;

-- 2 Top 5 devices with highest average carbon monoxide levels.
SELECT device_id, AVG(carbon_monoxide)
FROM cleaned_environment
GROUP BY device_id
ORDER BY AVG(carbon_monoxide) DESC
LIMIT 5;

-- 3️ Average temperature recorded in the dataset.
SELECT AVG(temperature)
FROM cleaned_environment;

--4️ Timestamp and temperature of highest recorded temperature for each device.
SELECT device_id, MAX(temperature) , timestamp
FROM cleaned_environment
GROUP BY  device_id;


--5️ Devices where temperature increased from minimum to maximum recorded.
SELECT device_id 
FROM cleaned_environment
GROUP BY device_id
HAVING MAX(temperature) > MIN(temperature);

--6️ Exponential moving average of temperature for each device (limited to 10 devices).
SELECT
    device_id,
    timestamp,
    temperature,
    AVG(temperature) OVER (PARTITION BY device_id ORDER BY timestamp ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS ema_temperature
FROM
    cleaned_environment
LIMIT 10;


--7️ Timestamps and devices with carbon monoxide levels exceeding average.
SELECT timestamp, device_id
FROM cleaned_environment
WHERE carbon_monoxide> (SELECT AVG(carbon_monoxide) FROM cleaned_environment);



--8️ Devices with highest average temperature recorded.
SELECT device_id, MAX(avg_temp) AS max_avg_temp
FROM (
  SELECT device_id, AVG(temperature) AS avg_temp
  FROM cleaned_environment
  GROUP BY device_id
) AS temp_avg_table group by 1;

--9️ Average temperature for each hour of the day across devices.
SELECT HOUR(timestamp) AS hour_of_day, AVG(temperature) AS average_temperature
FROM cleaned_environment
GROUP BY hour_of_day
ORDER BY hour_of_day;


--10 Device(s) with only a single distinct temperature value.
SELECT HOUR(timestamp) AS hour_of_day, AVG(temperature) AS average_temperature
FROM cleaned_environment
GROUP BY hour_of_day
ORDER BY hour_of_day;


--1️1️ Devices with highest humidity levels.
SELECT device_id, MAX(humidity)
FROM cleaned_environment
GROUP BY device_id;


--1️2️ Average temperature for each device, excluding outliers.
SELECT ce.device_id, AVG(ce.temperature) AS average_temperature
FROM (
    SELECT device_id, temperature
    FROM cleaned_environment
) ce
WHERE ce.temperature BETWEEN (
        SELECT AVG(temperature) - 3 * STDDEV(temperature)
        FROM cleaned_environment
    )
    AND (
        SELECT AVG(temperature) + 3 * STDDEV(temperature)
        FROM cleaned_environment
    )
GROUP BY ce.device_id;


--1️3️ Devices that experienced sudden change in humidity (>50% difference) within 30 minutes.
SELECT table1.device_id, table1.timestamp, table1.humidity
FROM
(SELECT device_id, timestamp,
humidity,
ABS((humidity - (LAG(humidity,1) OVER (
PARTITION BY device_id
ORDER BY timestamp)))*100) c1
FROM `cleaned_environment`) table1
WHERE table1.c1 > 50;



--1️4️ Average temperature for each device during weekdays and weekends.
SELECT
    device_id,
    CASE WHEN DAYOFWEEK(timestamp) IN (1, 7) THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    AVG(temperature) AS average_temperature
FROM
    cleaned_environment
GROUP BY
    device_id,
    day_type;


--1️5️ Cumulative sum of temperature for each device, ordered by timestamp (limited to 10 records)
SELECT
    device_id,
    timestamp,
    temperature,
    SUM(temperature) OVER (PARTITION BY device_id ORDER BY (timestamp)) AS cumulative_temperature
FROM
    cleaned_environment
ORDER BY device_id
LIMIT 10;
