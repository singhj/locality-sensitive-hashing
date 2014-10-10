About this directory
=============

Purpose
-------

To prepare the data for loading into Blobstore in a form that is good for Map/Reduce

To run it,

```
prepare_blobstore_zips abc.zip def.zip
```

For each file on the command line, it will create a corresponding chunked.zip file (`abc.chunked.zip`, etc).
The chunked.zip file will contain `.txt` files for each ID that has html.
