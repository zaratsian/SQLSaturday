
###############################################################################################################
#
#   Spark - Load Data
#
###############################################################################################################

mm_season = spark.read.load("hdfs://sandbox.hortonworks.com:8020/tmp/marchmadness/SeasonResults/SeasonResults.csv", format="csv", header=True)

mm_season.show()
mm_season.count()
mm_season.dtypes

#registerDataFrameAsTable(mm_season, 'mm_season_sql')
mm_season.createOrReplaceTempView('mm_season_sql')

spark.sql('''   
    select ID,SEASON,WTEAM,LTEAM from mm_season_sql
    ''').show(10)

###############################################################################################################

mm_teams = spark.read.load("hdfs://sandbox.hortonworks.com:8020/tmp/marchmadness/Teams/Teams.csv", format="csv", header=True)

mm_teams.show()
mm_teams.count()
mm_teams.dtypes

#registerDataFrameAsTable(mm_teams, 'mm_teams_sql')
mm_teams.createOrReplaceTempView('mm_teams_sql')

###############################################################################################################
#
#   Spark - Join (as materialized table)
#
###############################################################################################################

mm_join1 = spark.sql('''                                                                                                                       
    SELECT mm_season_sql.*, 
        teams2a.team_name AS WTEAM_NAME, 
        teams2b.team_name AS LTEAM_NAME
    FROM mm_season_sql
    LEFT JOIN mm_teams_sql teams2a ON (mm_season_sql.wteam = teams2a.team_id)
    LEFT JOIN mm_teams_sql teams2b ON (mm_season_sql.lteam = teams2b.team_id)
''')

mm_join1.show()

###############################################################################################################
#
#   Spark - Join (as view)
#
###############################################################################################################

mm_join1.createOrReplaceTempView('mm_join1_sql')


###############################################################################################################
#
#   Spark - Calculations
#
###############################################################################################################

# Calculate the Top 15 Teams with the most Wins
spark.sql('''  
    SELECT WTEAM_NAME, COUNT(*) AS WINS 
        FROM mm_join1_sql 
        GROUP BY WTEAM_NAME 
        ORDER BY WINS DESC
    ''').show(15)


# Calculate the Top 15 Teams with the most Losses
spark.sql('''  
    SELECT LTEAM_NAME, COUNT(*) AS LOSSES 
        FROM mm_join1_sql 
        GROUP BY LTEAM_NAME 
        ORDER BY LOSSES DESC 
    ''').show(15)


# Calculate the Top 15 Matchups with the biggest score difference
spark.sql('''  
    SELECT SEASON, WSCORE, LSCORE, WLOC, (WSCORE-LSCORE) AS SCORE_DIFF, WTEAM_NAME, LTEAM_NAME
        FROM mm_join1_sql
        ORDER BY SCORE_DIFF DESC
    ''').show(15,False)



###############################################################################################################
#
#   Spark - Execute Job Against Hive Table
#
###############################################################################################################


from pyspark.sql import HiveContext

hive_context = HiveContext(sc)

hive_context.sql('show tables').show(25,False)

sample = hive_context.table("default.sample_07")
sample.show(10,False)

sample.registerTempTable("sample_temp")
hive_context.sql('show tables').show(25,False)
hive_context.sql("select * from sample_temp").show()



###############################################################################################################
#
#   Spark - Execute Job Against Phoenix Table
#
###############################################################################################################

/usr/hdp/2.5.0.0-1245/spark2/bin/pyspark --jars /usr/hdp/2.5.0.0-1245/phoenix/lib/phoenix-spark-4.7.0.2.5.0.0-1245.jar

/usr/hdp/2.5.0.0-1245/spark2/bin/pyspark --jars /usr/hdp/2.5.0.0-1245/phoenix/phoenix-4.7.0.2.5.0.0-1245-client.jar

/usr/hdp/2.5.0.0-1245/spark2/bin/pyspark --jars /usr/hdp/2.5.0.0-1245/phoenix/phoenix-4.7.0.2.5.0.0-1245-client.jar,/usr/hdp/2.5.0.0-1245/phoenix/lib/phoenix-spark-4.7.0.2.5.0.0-1245.jar --conf "spark.executor.extraClassPath=/usr/hdp/2.5.0.0-1245/phoenix/phoenix-4.7.0.2.5.0.0-1245-client.jar,/usr/hdp/2.5.0.0-1245/phoenix/lib/phoenix-spark-4.7.0.2.5.0.0-1245.jar"

# Read from Phoenix:
phoenixdata = spark.read                                                \
  .format("org.apache.phoenix.spark")                                   \
  .option("table", "AIRLINE_REVIEWS")                                   \
  .option("zkUrl", "sandbox.hortonworks.com:2181/hbase-unsecure")       \
  .load()



/usr/hdp/2.5.0.0-1245/spark2/bin/pyspark --driver-class-path /usr/hdp/current/phoenix-client/phoenix-4.7.0.2.5.0.0-1245-client.jar  --jars /usr/hdp/current/phoenix-client/phoenix-4.7.0.2.5.0.0-1245-client.jar

phoenixdata = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:phoenix:sandbox.hortonworks.com:2181:/hbase") \
    .option("dbtable", "AIRLINE_REVIEWS") \
    .load()




df = spark.read.format("org.apache.phoenix.spark").option("zkUrl", "phoenix.dev:2181/hbase").option("table", "mytable").load()



# Write to Phoenix:


#ZEND
