import logging,findspark,glob,os,numpy as np
findspark.init()
import pyspark
from time import sleep
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import *
from PIL import Image
from pathlib import Path
from wordcloud import WordCloud, STOPWORDS
from pyspark.ml import Pipeline
from sparknlp.base import Finisher, DocumentAssembler
from sparknlp.annotator import *
from sparknlp.pretrained import PretrainedPipeline

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

pretrained_model_ref = {"GB":["stopwords_en","en"],"US":["stopwords_en","en"],"CA":["stopwords_en","en"],"FR":["stopwords_fr","fr"],"JP":["stopwords_jp","jp"],"KR":["stopwords_kr","kr"]\
                        ,"RU":["stopwords_ru","ru"],"DE":["stopwords_de","de"],"BR":["stopwords_br","br"]}

output_dir = f"../output/"
data_dir = f"../data/"

def create_spark_session(description):
    logger.info("Reading Spark object")
    spark = SparkSession.builder.appName(description).config("spark.jars.packages", "com.johnsnowlabs.nlp:spark-nlp_2.12:3.4.0").getOrCreate()
    logger.info("Spark read ok")
    
    return spark

def execute_step(spark,csv):   
    prefix = Path(csv).name[0:2]
    
    logger.info("Reading csv to dataframe using csv_schema")

    df_pyspark = spark.read.csv(csv, header=True)

    df_title = df_pyspark.select("title")
    
    pretrained_model = pretrained_model_ref[prefix]
    
    ############################################################
    #build pipeline
    ############################################################
    logger.info("setting Pipeline")
    documentAssembler = DocumentAssembler().setInputCol("title").setOutputCol("document")
    tokenizer = Tokenizer().setInputCols(["document"]).setOutputCol("token")
    normalizer = Normalizer().setInputCols(["token"]).setOutputCol("normalized").setLowercase(True)
    lemmatizer = LemmatizerModel .pretrained().setInputCols(["normalized"]).setOutputCol("lemma")
    stop_words = StopWordsCleaner.pretrained(pretrained_model[0], pretrained_model[1]).setInputCols(["lemma"]).setOutputCol("clean_lemma").setCaseSensitive(False)
    finisher = Finisher().setInputCols(["clean_lemma"]).setCleanAnnotations(False)

    logger.info("set stage")
    pipeline = Pipeline().setStages([
                        documentAssembler,
                        tokenizer,
                        normalizer,
                        lemmatizer,
                        stop_words,
                        finisher
                        ])

    logger.info("calling pipeline")
    clean_word = pipeline.fit(df_title).transform(df_title)
    clean_word = clean_word.withColumn("keyword", F.explode(F.col("finished_clean_lemma")))
    clean_word = clean_word.select("title","keyword")

    ############################################################
    #generate word cloud
    ############################################################
    
    logger.info("Start generating word cloud")
    youtube_image = np.array(Image.open(data_dir+"youtube.png"))
    clean_text = " ".join([str(row["keyword"]) for row in clean_word.collect()])
    wc = WordCloud(background_color="white", max_words=1000, mask=youtube_image)
    wc.generate(clean_text)
    wc.to_file(output_dir+prefix+"_wordcloud.png")
    
    
    
if __name__ == "__main__":
    csv_list = [i for i in (Path(data_dir).glob("*.csv"))]
    
    session = create_spark_session("nlp word cloud generation")
    for csv in csv_list:
        if csv.name[0:2] not in pretrained_model_ref:
            logger.info("Inappropriate file name in folder, abort now")
            break
        else:
            execute_step(session,str(csv))
            sleep(5)
            #clean up file
            os.remove(csv)
            
    
        
