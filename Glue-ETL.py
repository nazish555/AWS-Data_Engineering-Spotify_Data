import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Albums
Albums_node1711551541652 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-project-nazish/staging/albums.csv"], "recurse": True}, transformation_ctx="Albums_node1711551541652")

# Script generated for node Tracks
Tracks_node1711551542702 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-project-nazish/staging/track.csv"], "recurse": True}, transformation_ctx="Tracks_node1711551542702")

# Script generated for node Artists
Artists_node1711551541172 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://spotify-project-nazish/staging/artists.csv"], "recurse": True}, transformation_ctx="Artists_node1711551541172")

# Script generated for node Artists & Albums Join
ArtistsAlbumsJoin_node1711551783021 = Join.apply(frame1=Artists_node1711551541172, frame2=Albums_node1711551541652, keys1=["id"], keys2=["artist_id"], transformation_ctx="ArtistsAlbumsJoin_node1711551783021")

# Script generated for node Join with Tracks
JoinwithTracks_node1711551551779 = Join.apply(frame1=Tracks_node1711551542702, frame2=ArtistsAlbumsJoin_node1711551783021, keys1=["track_id"], keys2=["track_id"], transformation_ctx="JoinwithTracks_node1711551551779")

# Script generated for node Drop Fields
DropFields_node1711552126140 = DropFields.apply(frame=JoinwithTracks_node1711551551779, paths=["`.track_id`", "id"], transformation_ctx="DropFields_node1711552126140")

# Script generated for node Amazon S3
AmazonS3_node1711552147086 = glueContext.write_dynamic_frame.from_options(frame=DropFields_node1711552126140, connection_type="s3", format="glueparquet", connection_options={"path": "s3://spotify-project-nazish/datawarehouse/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="AmazonS3_node1711552147086")

job.commit()