from pyspark import SparkConf, SparkContext

def main():
    # Initialize a SparkConf and SparkContext
    conf = SparkConf().setAppName("AverageRating")
    sc = SparkContext(conf=conf)

    # Load the data from the CSV file
    # Assume the format: some_column,movieid,rating (with header row)
    lines = sc.textFile("ratings.csv")

    # Skip the header row
    header = lines.first()
    lines = lines.filter(lambda line: line != header)

    # Map step: Emit (movieid, (rating, 1))
    movie_ratings = lines.map(lambda line: line.split(',')) \
                         .map(lambda fields: (fields[1], (float(fields[2]), 1)))

    # Reduce step: Aggregate ratings by movieid and compute total ratings and counts
    rating_sums_counts = movie_ratings.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))

    # Map step: Compute average rating
    average_ratings = rating_sums_counts.mapValues(lambda x: x[0] / x[1])

    # Save the results to a text file
    average_ratings.saveAsTextFile("average_ratings_output")

    # Stop the SparkContext
    sc.stop()

if __name__ == "__main__":
    main()
