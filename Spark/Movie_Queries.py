from pyspark.sql.types import *
from pyspark.sql.functions import split, explode
from pyspark.sql.window import *
from pyspark.sql.functions import *

#defining schema for movies table
schema = (StructType().add("movieId",IntegerType()).add("title",StringType()).add("genres",StringType()))
#importing data from movies.csv with above specified schema and indicating that first fields are column headers
movies_df = spark.read.csv("file:/home/hadoop/ml-25m/movies.csv",schema=schema,header=True)
#split entries for movies classified under multiple genres into multiple single-genre entries
movies_df=movies_df.withColumn("genres", explode(split("genres","[|]")))
#import data from ratings.csv, indicating that the first fields are column headers
ratings_df=spark.read.csv("file:/home/hadoop/ml-25m/ratings.csv",header=True)
#join movies and ratings table with movieId as the common field
movie_ratings_df=movies_df.join(ratings_df,movies_df.movieId==ratings_df.movieId).drop(ratings_df.movieId)

#assign a number 'rank' to each single-genre entry for a particular movie
temp_df=movie_ratings_df.withColumn("rank",row_number().over(Window.partitionBy("userId","title").orderBy("rating")))
#keep only one single-genre entry for each movie (entries having rank 1) and discard other entries
temp_df=temp_df.filter(temp_df.rank==1)
#drop the rank column used to filter out surplus entries for the same movie
temp_df=temp_df.drop('rank')

#convert the string type entries in the rating column to float type
temp_df=temp_df.withColumn('rating',temp_df['rating'].cast("float").alias('rating'))
#calculate average rating for each movie
avg_df=temp_df.groupBy('movieId','title','genres').avg('rating')
avg_df.show(10)

#count the number of ratings for each movie
count_df=temp_df.groupBy('movieId','title').count().withColumnRenamed('count','ratingCount')
#keep the entries for movies which have been rated by a minimum of 50 users and discard other entries
count_df1=count_df.filter(count_df.ratingCount>50)
#drop the title and ratingCount columns
count_df1=count_df1.drop('ratingCount','title')
#join count_df1 and avg_df dataframes over the movieId column
finaldf = count_df1.join(avg_df,on='movieId')

#count number of movies in each genre
movies_df.groupBy('genres').count().show()
#list movies with no genres listed
movies_df.filter(movies_df.genres=="(no genres listed)").show()
#put user ID and find top 10 best rated movies for that user
temp_df.where((temp_df.userId=='100004')&(temp_df.rating=='5.0')).show(10)
#put user ID and find top 10 worst rated movies for that user
temp_df.where((temp_df.userId=='10')&(temp_df.rating=='1.0')).show(10)
#top 10 movies overall
finaldf.orderBy('avg(rating)',ascending=False).show(10)
#worst 10 movies overall
finaldf.orderBy('avg(rating)').show(10)
#top 10 movies of Thriller genre
finaldf.where(finaldf.genres=='Thriller').orderBy('avg(rating)',ascending=False).show(10)
#worst 10 movies of Thriller genre
finaldf.where(finaldf.genres=='Thriller').orderBy('avg(rating)').show(10)