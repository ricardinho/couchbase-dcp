COUCHBASE DCP CLIENT
====================

Client for couchbase DCP protocol written in twisted.

To run:
 * Create a secrets.py file in the root folder
 * Create inside the file a variable called DCP_INSTANCE that will hold a tuple
   with the ip and port of the dcp server
 * create a variable called DCP_CREDENTIALS that holds a tuple with the name of
   the bucket and the password


As of right now, this code will run only on vbucket 0.

