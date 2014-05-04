OpenLSH
=======

Stages
------

OpenLSH is an open source platform that implements Locality Sensitive Hashing. It encompasses an end-to-end architecture comprising the following _stages_:
 1. Data mining from social media: Twitter, LinkedIn, Github, &hellip;,
 2. Filtering incoming data,
 3. Shingling,
 4. Minhashes,
 5. Locality sensitive hashing and
 6. Candidate matching.

The OpenLSH framework is designed with a pipelining architecture and be extensible.

Flexible Pipelining
-------------------

Each of the stages listed above is implemented using operators which can run as independent threads. 
Each operator can be implemented as an iterator, which is class with three three methods that allows a consumer of the result of the physical operator to get the result one _item_ at a time. The three methods forming the iterator
for an operation are:

 1. 0pen (). This method starts the process of getting items, but does not get
    an item. It initializes any data structures needed to perform the operation
    and calls 0pen() for any arguments of the operation.
 2. GetNext (). This method returns the next item in the result and adjusts
    data structures as necessary to allow subsequent items to be obtained.
    In getting the next item of its result, it typically calls GetNext() one
    or more times on its argument(s). If there are no more items to return,
    GetNext () returns a special value NotFound, which we assume cannot be
    mistaken for a item.
 3. Close (). This method ends the iteration after all items, or all items that the consumer wanted, have been obtained. Typically, it calls Close () on any arguments of the operator.

`lshIterator` is a base class from which the above stages are derived. It defines Open(), GetNext(), and
Close() methods on instances of the class. Each stage class, in turn, provides a default implementation for its function (mining, filtering, etc). The flexibility of the OpenLSH framework comes from the fact that the the default base classes representing each stage can be further inherited and modified for specific implementations.

The code, especially the GetNext () method, makes heavy use of the `yield` command in Python. In case you are not familiar with it, [here is an explanation] (http://stackoverflow.com/questions/231767/the-python-yield-keyword-explained).

The first implementation will be based on Google App Engine and written in Python. The data will be stored as shown:

| Stage        | Data Storage for Results                                |
|:-------------|:--------------------------------------------------------|
| `mining`     | Blobstore                                               |
| `filtering`  | Blobstore                                               |
| `shingling`  | Blobstore                                               |
| `minhash`    | Datastore                                               |
| `LSH buckets`| Datastore                                               |
| `matching`   | Datastore                                               |


Implementation thus far
-----------------------

The problems we had with using the streaming API have been resolved and the code has been updated.

To read tweets,
 1. Download the repo
 2. Get your own consumer_key and consumer_secret from the Twitter App [Registration page](https://apps.twitter.com/).
    - The consumer keys are your application api key and secret key.
 3. Set up callback URL appropriately.
 4. Change application id in app.yaml and push the code
    - If you set up 2-step Verification on your google user account you will need to create an application specific password. This will be the application you use when you deploy your GAE app.
    - Go to more info: https://support.google.com/accounts/answer/185833?hl=en for more info.
 5. Visit <your application id>.appspot.com/get_tweets in your favorite browser.
 6. You will need to give permission to invoke the Twitter API on your behalf
 7. The tweets will be visible in the logs. Go to https://appengine.google.com/ and navigate to logs for your application.

Testing
----------

We are using nose and mock to aid with unit testing various OpenLSH modules. These libraries
are included in the project as they are needed by Google App Engine. You will need to install these
on your local development machine as well.

To install testing libraries (assumes you have pip installed) from the command line type the following commands:
* install nose: pip install nose
* install mock: pip install mock

To run all tests:
 1. From the command line navigate to the to the /tests in each package.
 2. Type the following command: nosetests [test_pythone_file_name].py

To run individual test method:
 1. From the command line navigate to the to the /tests in each package.
 2. Type the following command: nosetests [test_pythone_file_name].py:test_method_name


References
----------

 1. [LSH Presentation at Boston Data Mining Meetup](http://www.slideshare.net/j_singh/mining-of-massive-datasets-using-locality-sensitive-hashing-lsh).
 2. [Terasa's blog post about this project](http://quarksandbits.com/en/2014/04/23/using-twitter-streaming-api-google-app-engine/).
 3. [Mining of Massive Datasets book](http://infolab.stanford.edu/~ullman/mmds/book.pdf).
