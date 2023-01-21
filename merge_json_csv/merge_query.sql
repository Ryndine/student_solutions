-- We will need to create a type to be used to unpack our json
DROP TYPE IF EXISTS feature_rows;
CREATE TYPE feature_rows AS (
    "type" VARCHAR,
    "properties" JSON,
    "geometry" JSON
);

-- Using SQLAlchemy to import json data
-- copy doesn't support this json format.
SELECT * FROM json_table;

-- Table for the csv
DROP TABLE IF EXISTS visuals_table;
CREATE TABLE visuals_table (
    fips INT,
    recent_trend TEXT,
    prediction_trend TEXT,
    max_pred DECIMAL(6,8),
    pm_max_pred DECIMAL(6,8)
)
--insert csv data
COPY visuals_table (fips, recent_trend, prediction_trend, max_pred, pm_max_pred) 
FROM 'C:\Users\rcman\Desktop\GitHub_Repositories\student_solutions\merge_json_csv\visual_date.csv' 
    WITH (FORMAT csv, HEADER true, DELIMITER ',');

SELECT * FROM visuals_table;

-- The big ugly query to join it all together
WITH geo_table (geo, state, county, name, lsad, censusarea, type, coordinates)
	AS 
    (
        SELECT 
            -- need to convert types from json
            CAST(properties->>'GEO_ID' AS VARCHAR) AS geo_id,
            LPAD(properties->>'STATE'::TEXT, 2, '0') AS state,
            LPAD(properties->>'COUNTY'::TEXT, 3, '0') AS county,
            CAST(properties->>'NAME' AS TEXT) AS name,
            CAST(properties->>'LSAD' AS TEXT) AS lsad,
            CAST(properties->>'CENSUSAREA' AS DECIMAL(10,2)) AS censusarea,
            CAST(geometry->>'type' AS TEXT) AS type,
            CAST(geometry->>'coordinates' AS JSON) AS coordinates
        FROM 
        (
            -- json_populate_recordset() will convert a list of dicts to Column/Row
            -- pass through the data types for your keys, then the list of dicts
            -- don't forget to wrap in () and use (data).* to populate columns
            SELECT (json_populate_recordset(null::feature_rows, json_table.data->'features')).*
            FROM json_table
        ) AS data_table 
    )
SELECT 
    geo.geo,
    vis.fips,
    geo.state,
    geo.county,
    geo.name,
    geo.lsad,
    geo.censusarea,
    vis.recent_trend,
    vis.prediction_trend,
    vis.max_pred,
    vis.pm_max_pred,
    geo.type,
    geo.coordinates
FROM geo_table as geo
INNER JOIN visuals_table as vis
ON right(geo.geo, 5) = LPAD(vis.fips::text, 5, '0');
