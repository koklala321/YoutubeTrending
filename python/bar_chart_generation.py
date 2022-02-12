import logging,json,findspark
findspark.init()
import pyspark
from time import sleep
from pathlib import Path
from pyspark.sql import SparkSession,Window
import pyspark.sql.functions as F
from pyspark.sql.types import *
import matplotlib.pyplot as plt
import seaborn as sns
#%matplotlib inline


logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
prefix_check = ["GB","US","CA","FR","JP","KR","RU","DE","BR"]
output_dir = f"../output/"
data_dir = f"../data/"

def create_spark_session(description):
    logger.info("Reading Spark object")
    spark = SparkSession.builder.appName(description).getOrCreate()
    logger.info("Spark read ok")
    
    return spark

def process_csv(spark,csv):

    prefix  = Path(csv).name[0:2]
    
    csv_schema = StructType([StructField("video_id",StringType(),False),
                            StructField("title",StringType(),True),
                            StructField("publishedAt",DateType(),True),
                            StructField("channelId",StringType(),True),
                            StructField("channelTitle",StringType(),True),
                            StructField("categoryId",ShortType(),True),
                            StructField("trending_date",DateType(),True),
                            StructField("tags",StringType(),True),
                            StructField("view_count",LongType(),True),
                            StructField("likes",LongType(),True),
                            StructField("dislikes",LongType(),True),
                            StructField("comment_count",LongType(),True),
                            StructField("comments_disabled",BooleanType(),True),
                            StructField("ratings_disabled",BooleanType(),True),
                            StructField("description",StringType(),True),
                            StructField("country",StringType(),True)]
                        )
    
    logger.info("Reading csv to dataframe using csv_schema")
    df_pyspark = spark.read.csv(csv, header=True,schema=csv_schema)

    logger.info("transforming dataframe")

    ############################################################
    #add trend count to df
    ############################################################
    trending_count = df_pyspark.groupby("video_id").count().withColumnRenamed("count","trendCount")

    df_pyspark = df_pyspark.join(trending_count,"video_id").select(df_pyspark["*"],trending_count["trendCount"])
    
    
    #use window function to drop all duplicate row
    win = Window.partitionBy("video_id").orderBy("trending_date")
    
    df_pyspark = df_pyspark.withColumn('rank', F.row_number().over(Window.partitionBy(df_pyspark.video_id).orderBy(df_pyspark.trending_date.desc())))\
                        .filter(F.col('rank') == 1)\
                        .drop("rank")

    
    ############################################################
    #read category name from json and add to df
    ############################################################
    logger.info("read category json")
    category_id = {}
    try:
        with open("../data/"+prefix+"_category_id.json","r") as f:
            category = json.load(f)
    except:
        logger.info("Error at loading/reading json, check if json file exist")
        return
    else:
        for cat in category["items"]:
            category_id[int(cat["id"])] = cat["snippet"]["title"]
            
    cat = list(map(list, category_id.items()))

    category = spark.createDataFrame(cat, ["categoryId", "categoryName"])

    logger.info("add category name to df")
    
    df_pyspark = df_pyspark.join(category,"categoryId").select(df_pyspark["*"], category["categoryName"])
    
    logger.info("create cat like")   
    
    cat_like = df_pyspark.groupby("categoryName")\
                        .sum("likes","dislikes")\
                        .withColumnRenamed("sum(likes)","totalLikes")\
                        .withColumnRenamed("sum(dislikes)","totalDislikes")\
                        .withColumn("totalDislikes",F.col("totalDislikes")+1)\
                        .withColumn("liketoDislike",((F.col("totalLikes"))/(F.col("totalDislikes"))).cast(IntegerType()))\
                        .orderBy("totalLikes",ascending=False)

    logger.info("to pandas")   

    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    
    ############################################################
    #create graph for like to dislike vidoe category
    ############################################################
    logger.info("start plotting most_liked_category")
    
    plt.figure(figsize=(16, 14))
    plt.title("Like to dislike ratio for each category in " + prefix,fontsize = 24,fontweight = "bold")
    plt.ticklabel_format(style="plain",useOffset=False)
    sns.color_palette("light:b", as_cmap=True)
    ax = sns.barplot(x="liketoDislike", y="categoryName", data=cat_like.toPandas(), color="c")
    ax.set_xlabel ("Likes-Dislikes Ratio",fontsize = 16)
    ax.set_ylabel ("Category Name",fontsize = 16)

    plt.savefig(output_dir+prefix+"_most_liked_category.png")
    
    ############################################################
    #create graph for most trending category base on view count
    ############################################################
    logger.info("start plotting most_trending _category")
    
    cat_trend = df_pyspark.groupby("categoryName").count().orderBy("count",ascending=False)
    plt.clf()
    plt.figure(figsize=(16, 14))
    plt.title("Most trending video category in " + prefix,fontsize = 24,fontweight = "bold")
    plt.ticklabel_format(style='plain',useOffset=False)
    sns.color_palette("flare", as_cmap=True)
    ax = sns.barplot(x="count", y="categoryName", data=cat_trend.toPandas(),orient='h')
    ax.set_xlabel ("Number of trending video",fontsize = 16)
    ax.set_ylabel ("Category Name",fontsize = 16)

    plt.savefig(output_dir+prefix+"_most_trending_category.png")

    
if __name__ == "__main__":
    
    csv_list = [i for i in (Path(data_dir).glob("*.csv"))]
    
    session = create_spark_session("nlp word cloud generation")
    for csv in csv_list:
        if csv.name[0:2] not in prefix_check:
            logger.info("Inappropriate file name in folder, abort now")
            break
        else:
            process_csv(session,str(csv))
            sleep(5)
