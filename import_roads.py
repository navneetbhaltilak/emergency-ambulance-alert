import geopandas as gpd
from sqlalchemy import create_engine

gdf = gpd.read_file("roads.geojson")
gdf = gdf.rename_geometry("geom")

# Keep only the columns that match your table schema
gdf = gdf[["geom"]].copy()
gdf["name"] = None  # add a placeholder name column since your table has one

engine = create_engine("postgresql://postgres.afofefvzoiltroekxucc:Navneet%40123@aws-0-ap-northeast-2.pooler.supabase.com:5432/postgres")
gdf.to_postgis("roads", engine, if_exists="append", index=False)

print(f"Imported {len(gdf)} road segments successfully.")