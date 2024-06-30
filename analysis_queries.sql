-- here table is weather

-- Daily Min Temp by City
SELECT city, date, mintemp FROM weather WHERE date = CURRENT_DATE - INTERVAL '1 day' ORDER BY city;

-- Daily Max Temp by City
SELECT city, date, maxtemp FROM weather WHERE date = CURRENT_DATE - INTERVAL '1 day' ORDER BY city;

-- Daily Sunset by City
SELECT city, date, sunset FROM weather WHERE date = CURRENT_DATE - INTERVAL '1 day' ORDER BY city;

--  Daily Sunrise by City
SELECT city, date, sunrise FROM weather WHERE date = CURRENT_DATE - INTERVAL '1 day' ORDER BY city;

--  Hourly Temp by City
SELECT city, date, hourly FROM weather WHERE date = CURRENT_DATE - INTERVAL '1 day' ORDER BY city;



