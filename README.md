![Yggdrasil.jpg is an image from the Wikimedia Commons](http://jamesmcclain.info/TOPK/Yggdrasil.jpg)

# worldtree

A Clojure library/program to do Top-k queries on large time series data sets.
The ideas behind this program are presented in the
[article](http://jamesmcclain.info/TOPK/)
that accompanies this project.

## Usage

Begin a REPL session by typing `lein repl` at the shell prompt while in the project directory or by bulding and running the jar file and connecting to it on port 4005 via nREPL.

From within a REPL session, type

     $ (ns worldtree.core)

to navigate into the main project namespace.

### Loading Data

You must first generate or load the data that you wish to work on.

If you would like to work on randomly-generated data, you can type something like

     $ (def rawdata (data/random-data 10000 512 30))

to generate a data set with 10,000 time series, each of length 512, whose magnitudes have standard deviation 30 about their respective means.
The variable `rawdata` points to a vector of length 10,000 and each of its entries is a vector of length 512.

If you would like to work with real data, that can be accomplished by typing something like

     $ (def rawdata (data/row-major-load "/tmp/mallat.data.xz" :column-drop 1 :column-drop-end 2))

or maybe

     $ (def rawdata (data/row-major-load "/tmp/lighcurves.data" :column-drop 1))
     
to load the data from a file.

The `data/row-major-load` function loads text data sets in row-major format from disk and turns them into a collection of collections that is similar to that produced by `data/random-data`.
In the case of the *Mallat* data set, the result is a vector of length 2400 (because there are that many time series), and each time series is of length 1024 (because there are that many time steps in each series).
The loader is capable of transparently decompressing data sets that have been compressed by `gzip`, `bzip2`, or `xz`.
The *Mallat* data set has an extra column of non-data at the left and two columns of non-data at the right of each row, that information is conveyed to the loader with `:column-drop 1` and `:column-drop-end 2`.

Once the raw data have been loaded, they need to be transformed into the program's special data set format.
That is done by typing

     $ (def dataset (data/row-major-dataset rawdata))

which assigns the data set to a varible called `dataset`.

It is assumed that all time series within a data set contain the same number of time steps and that all of those steps are of equal length.
At present, there are only subroutines to work with row-major data sets, but adding support for column-major sets should not be difficult.

### Building a Search Index

Once the data have been loaded, the search index can be built by typing

     $ (index/build dataset "/tmp/M/")
     
to invoke the `index/build` function.
The search index over `dataset` will be placed in the directory `/tmp/M/`.

### Querying A Search Index

The search index `/tmp/M/` can be queried in something like the following way

     $ (index/query "/tmp/M/" 256 33)

where the search index is queried for *k=256* at time *t=33*.
The parameter *k* must be a power of two *≥ 2* and *≤ n/2*.
If the data set contains 1024 time steps, then the parameter *t* must be a real number in the interval *[0,1023)*.

## Replication of Experiments

Detailed instructions on how to replicate the experiments reported on in the article can be found
[here](http://jamesmcclain.info/TOPK/replication.html).

## License

Copyright © 2014 James McClain

Distributed under the BSD-3 license; see the file COPYING.md in this distribution.

