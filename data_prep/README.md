About this directory
=============

Purpose
-------

To prepare the data for loading into Blobstore in a form that is good for Map/Reduce

To run it,

```
prepare_blobstore_zips abc.zip def.zip
```

For each file on the command line, it will create a corresponding blobstore.zip file (`abc.blobstore.zip`, etc).
In addition to the files in the original zip, the blobstore.zip file will contain `.txt` files for each ID that has html.
