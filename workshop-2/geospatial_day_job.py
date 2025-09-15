# %%
from sedona.spark import SedonaContext

config = SedonaContext.builder().getOrCreate()
sedona = SedonaContext.create(config)

# %%
catalog = 'matt'

# %%
study_area = 'POLYGON((-93.227839 45.12455, -92.984234 45.12455, -92.984234 44.88727, -93.227839 44.88727, -93.227839 45.12455))'

# %%
# Bronze Tables

# Overture Admin Units
sedona.sql(f'''
CREATE OR REPLACE TABLE wherobots.{catalog}.divison_areas_bronze
SELECT * 
FROM wherobots_open_data.overture_maps_foundation.divisions_division_area
WHERE ST_Intersects(
    geometry, 
    ST_GeomFromText('{study_area}'))
AND subtype IN ('locality', 'neighborhood')''')

# %%
# Buildings as Centroids

sedona.sql(f'''
CREATE OR REPLACE TABLE wherobots.{catalog}.buildings_bronze
SELECT id, class, height, names, ST_Centroid(geometry) as geometry
FROM wherobots_open_data.overture_maps_foundation.buildings_building
WHERE ST_Intersects(
    geometry, 
    ST_GeomFromText('{study_area}'))
''')

# %%
# Baseball Fields as Centroids

sedona.sql(f'''
CREATE OR REPLACE TABLE wherobots.{catalog}.field_centroids_bronze
SELECT
    confidence_scores,
    ST_GeomFromText(segments_wkt) as geometry
FROM
    wherobots.{catalog}.baseball_parks_sam preds
WHERE
    confidence_scores != 0.0

''')

# %%
# Silver Tables

# %%
# Buildings KNN Join

sedona.sql(f'''
CREATE OR REPLACE TABLE wherobots.{catalog}.buildings_knn_silver AS
SELECT
    ST_DistanceSphere(a.geometry, b.geometry) as distance,
    a.id as building_id, 
    a.geometry
FROM wherobots.{catalog}.buildings_bronze a 
JOIN wherobots.{catalog}.field_centroids_bronze b
ON ST_AKNN(a.geometry, b.geometry, 4, FALSE)
''')

# %%
# Gold Tables

# KNN Averages and Values

sedona.sql(f'''
CREATE OR REPLACE TABLE wherobots.{catalog}.neighborhoods_gold AS
SELECT 
    a.id,
    AVG(b.distance),
    a.geometry
FROM wherobots.{catalog}.divison_areas_bronze a
JOIN wherobots.{catalog}.buildings_knn_silver b
ON ST_Contains(a.geometry, b.geometry)
GROUP BY a.id, a.geometry
''')

# %%



