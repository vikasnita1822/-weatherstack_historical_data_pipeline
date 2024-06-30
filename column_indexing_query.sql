
-- creates an index named idx_city_date on the weather table based on the columns city and date in that order
CREATE INDEX idx_city_date ON weather (city, date);

-- creates an index named idx_date_city on the weather table based on the columns city and date in that order.
CREATE INDEX idx_date_city ON weather (date, city);


-- idx_date_city: This index will be used when the database needs to filter rows based on the date and then order them by city.
--  It is particularly useful because the primary filter is on the date, and then it sorts the result by city.

-- idx_city_date: This index will support queries that need to order by city and filter by date.
