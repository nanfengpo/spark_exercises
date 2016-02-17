"""Cluster tweets using k means algorithm in a Spark cluster."""

__author__ = 'Rohith Subramanyam <rohithvsm@cs.wisc.edu>'

import string

from nltk.corpus import stopwords
from nltk.stem.porter import PorterStemmer
from nltk.tokenize import word_tokenize
from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans
from pyspark.mllib.feature import HashingTF, IDF

stop_words = None
punctuations = None
STEMMER = PorterStemmer()


def _get_conf(name):
    """Set configuration parameters and return."""

    return (SparkConf().set('spark.app.name', name)
            .set('spark.master', 'spark://10.0.1.86:7077')
            .set('spark.driver.memory', '1g')
            .set('spark.eventLog.enabled', 'true')
            .set('spark.eventLog.dir',
                 '/home/ubuntu/storage/logs')
            .set('spark.executor.memory', '21g')
            .set('spark.executor.cores', '4')
            .set('spark.task.cpus', '1'))


def _tokenize(text):
    """Breaks the text into 'tokens', lowercase them, remove punctuation
    and stopwords, and stem them."""

    # Remove retweet keyword from the beginning of the tweet
    text = text.lstrip('rt')
    tokens = word_tokenize(text)
    lowercased = [t.lower() for t in tokens]
    no_punctuation = []
    for word in lowercased:
        # remove URLs
        if word.startswith('https://') or word.startswith('http://') or (
                word.startswith('www.')): continue
        # remove unicode (that represents emojis, etc.)
        if not word.decode('unicode_escape').encode('ascii', 'ignore'):
            continue
        # remove punctuations
        punct_removed = ''.join([letter for letter in word if letter not in
                                                           punctuations.value])
        no_punctuation.append(punct_removed)
    no_stopwords = [w for w in no_punctuation if w not in stop_words.value]
    # stems, stemmer, stemming, stemmed becomes stem
    stemmed = [STEMMER.stem(w) for w in no_stopwords]
    return [w for w in stemmed if w]


def save_model(sc, model, path):
    """Save the model on disk.

    Returns:
        path where the model is saved.

    """

    model.save(sc, path)

    return path


def top_n_in_each_cluster(sc, input_text_rdd, tfidf_vectors_rdd, model, n=5):
    """Find the top n most frequent words in each cluster."""

    cluster_words = input_text_rdd.zip(tfidf_vectors_rdd).map(lambda a: (
        model.predict(a[1]), a[0])).reduceByKey(lambda a, b: a + b).collect()
    for cluster_id, word_list in cluster_words:
        print '-' * 79
        print cluster_id
        print sc.parallelize(word_list).map(lambda x: (x, 1)).reduceByKey(
            lambda x, y: x + y).sortBy(lambda x: x[1], ascending=False).map(
            lambda a: (a[1], a[0])).top(n)


def build_cluster_model(tfidf_vectors_rdd, num_clusters, max_iterations, runs):
    """Perform the clustering of vectors using K-means.

    Returns:
        k means model learned from the training data in
            tfidf_vectors_rdd

    """

    # Build the model (cluster the training data)
    return KMeans.train(tfidf_vectors_rdd, num_clusters,
                        maxIterations=max_iterations, runs=runs)


def get_feature_vectors(sc, input_file, feature_dimensions):
    """Get feature vector from the lines in input_file_obj using
    TF/IDF.

    Returns:
        vectors RDD

    """

    # Load documents (one per line).
    tweet_file = sc.textFile(input_file)
    input_text_rdd = tweet_file.map(lambda line: _tokenize(line))
    input_text_rdd.cache()

    # The default feature dimension is 2^20; for a corpus with million
    # tweets recommended dimensions are 50000 or 100000. Use higher
    # dimensions for larger corpus of tweets.
    hashing_tf = HashingTF(feature_dimensions)
    tf = hashing_tf.transform(input_text_rdd)
    tf.cache()
    idf = IDF(minDocFreq=2).fit(tf)
    tfidf = idf.transform(tf)
    tfidf.cache()

    return input_text_rdd, tfidf


def _parse_cmd_line_args():
    """Parse command line argument.

    Returns:
        a tuple of input file path, number of clusters and feature
        dimensions

    """

    import argparse

    parser = argparse.ArgumentParser(description=(
        'Cluster the tweets in the input file using k-means Spark ML library'))

    parser.add_argument('--version', action='version', version='%(prog)s 1.0')
    parser.add_argument('input_file',  # type=argparse.FileType(),
                        help='path to input file containing tweets')
    parser.add_argument('--feature_dimension', '-d', type=int, default=50000,
                        help='Number of dimensions in the feature vector')
    parser.add_argument('--num_clusters', '-n', type=int, default=10,
                        help='Number of clusters')
    parser.add_argument('--max_iterations', '-i', type=int, default=100,
                        help='Maximum number of iterations of k means to run')
    parser.add_argument('--runs', '-r', type=int, default=1,
                        help='Number of times to run the k-means algorithm')

    args = parser.parse_args()

    return (args.input_file, args.feature_dimension, args.num_clusters,
            args.max_iterations, args.runs)


def main():
    """Process the input file got as a command-line argument."""

    global stop_words, punctuations

    input_file, feature_dimensions, num_clusters, max_iterations, runs = (
        _parse_cmd_line_args())

    sc = SparkContext(conf=_get_conf('CS-838-Assignment3-PartB'))

    # for the _tokenize function to remove stopwords and punctuations
    stop_words = sc.broadcast(set(stopwords.words('english')))
    punctuations = sc.broadcast(set(string.punctuation))

    input_text_rdd, tfidf_vectors_rdd = get_feature_vectors(sc, input_file,
                                                            feature_dimensions)
    model = build_cluster_model(tfidf_vectors_rdd, num_clusters,
                                max_iterations, runs)
    top_n_in_each_cluster(sc, input_text_rdd, tfidf_vectors_rdd, model, 5)


if __name__ == '__main__':
    main()
