OpenLSH
=======

OpenLSH is an open source platform that implements Locality Sensitive Hashing. 

Demo
----

To familiarize yourself with LSH, please visit the [demo site](http://open-lsh.datathinks.org).

To try it, you will need a Gmail-based account and a Twitter account. 
 1. Log in using your Google credentials.
 2. Log in using your Twitter credentials.
 3. Start by pressing the _Get Tweets_ button. It gets the most recent 200 tweets from the Twitter public stream and displays them.
 4. Press the _Run LSH_ button. It takes about a minute. When the analysis is complete, a _Show duplicate or near-duplicate tweets_ button appears.
 5. Press the _Show duplicate or near-duplicate tweets_ and it shows the tweets that were duplicates or near-duplicates.

That is the main idea of LSH: the incoming "Documents" &mdash; tweets in this case &mdash; are assigned to "buckets".
This can be done as documents come in from the source or all at once, as in the demo.
The resulting buckets can be examined and identical or similar documents just fall into the same buckets.
Each document will fall into multiple buckets.
It is not a perfect indicator but the number of buckets two documents have in common is suggestive of their similarity (more common buckets = more similarity).   

Architecture
------------

Central to OpenLSH is a sparse matrix data structure `Matrix`. 
It has as many rows as there are documents and as many columns as there are buckets (up to 2<sup>32</sup> buckets in this implementation).
A related data structure `MatrixRow` represents a document.

A _line format class_ specifies how to parse incoming documents. 
The line format class should specify a static method `parse` which returns a document ID and the text to be used for classification.
See classes `TweetLine` and `PeerbeltLine` in the code base as examples.
You may follow the implementation of PeerbeltLine as a template for parsing documents in your implementation &mdash; 
the original text is HTML and we strip out content inside `<script>` and `<style>` tags and also remove other HTML tags before assigning documents to buckets.
In this case, documents that have the same text but differ only in their markup are considered to be identical.

A _database_ specifies how to interact with the database. `settings.py` specifies which database is being used.
Implementations are available for Google App Engine Datastore and for Cassandra. 
An in-memory database based on Python data structures is also available and it is handy for testing.

Getting Started
---------------

To start, replicate the code in your own App Engine instance.

 1. Download the source code (`git clone https://github.com/singhj/locality-sensitive-hashing.git`).
 2. [Create a Twitter App] (https://apps.twitter.com/) and change `twitter_settings.py` as appropriate.
    Take care to specify a Callback URL correctly and 
    Ours is `http://open-lsh.datathinks.org/twitter_callback`, yours should be customized to your setup.
 3. Get yourself a [Google App Engine account] (https://cloud.google.com/appengine/docs) and register an application name.
 4. Change the application name in `app.yaml`.
 5. [Deploy] (https://cloud.google.com/appengine/docs/python/tools/uploadinganapp#Python_Uploading_the_app) to App Engine and verify.

Working from source data in a file, running from the command line
-----------------------------------------------------------------

Example Peerbelt data is provided in the `example_data` directory. To try OpenLSH against this file,

 1. Change `settings.py` to use the in-memory database.
 2. Run `serial.py` specifying the provided data file as input.

Changes for your use case
-------------------------

First, write a line format class for parsing your input data. 

Second, if you are working with a different database than Cassandra or Datastore,
you will need to write a database driver; we can help with this if required.

A command-line implementation is available in `serial.py`, which may be adapted if you will be running from the command line.
A web-based implementation is available in `read_tweepy.py`.
A map-reduce based implementation may be available soon, contact us for details.

The implementation is intended to be compact in its use of storage; we don't store intermediate results (i.e., minhash values) in the database.
To compute the similarity of two documents, we need to programmatically compare them. And we do.
You may wish to make a different trade-off, and uncomment the minhash lines in the implementation of `MatrixRow`.

Some of the LSH parameters are embedded in the `Matrix` constructor.

Pull Requests
-------------

If you are implementing another database driver, or other changes that would benefit others, please send a pull request.

References
----------

 1. [LSH Presentation at Boston Data Mining Meetup](http://www.slideshare.net/j_singh/mining-of-massive-datasets-using-locality-sensitive-hashing-lsh).
 2. [Terasa's blog post about this project](http://quarksandbits.com/en/2014/04/23/using-twitter-streaming-api-google-app-engine/).
 3. [Mining of Massive Datasets book](http://infolab.stanford.edu/~ullman/mmds/book.pdf).
