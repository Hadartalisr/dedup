Deduper
==========

This module provides a variable size chunking base on CDC algorithm. <br/>
This project was created as a final project for Tel Aviv University's course "Adv. Storage Systems" (Semester 2022A).


Documentation 
=======
Compiling the project is the same as the standard for Golang.
```
go get 
go build 
```
Using the Deduper is very simple:

- Dedupe
``` 
 ./Deduper -dedup ./input-file-name ./output-file-name 
```
- UnDedupe
``` 
 ./Deduper -undedup ./input-file-name ./output-file-name 
```
- Compare
``` 
 ./Deduper -compare ./input-file-name ./output-file-name 
```

Internal Implementation
=============
<h2>The compressed file data structure</h2>

* <h4>MetaData offset (4 bytes)</h4>The offset for the metadata.
* <h4>Chunks data</h4>Consist of the chunks' length (4 bytes) and the chunks' data.
* <h4>MetaData length (4 bytes)</h4>The number (in decimal) of offsets which should be written to the uncompressed file.
* <h4>Offsets</h4>Offsets in the file which should be written to the uncompressed file, ordered by the writing order.
<br/><br/>
  ![Alt text](./assets/compressedFileStructure.png?raw=true)

<h2>The chunking algorithm </h2> 


<h2>Intenral Data Structures</h2>

<h3>dedup</h3>
<h4>hashToOffset (map[uint32]int)</h4>
Used for storing the hash values for the chunks we have already seen before and stored in the compressed file.
The key in the file is a hash of a chunk and the value is its corresponding offset in the compressed file.

<h4>offset arrays ( int[] )</h4>
Used in order to store the metadata offsets in order. Each chunk results returns an offset
in the file which should be added to the uncompressed file output. 

<h4>startsSet ( map[string]struct{} ) </h4>Used in order to prevent calculating the hash for every byte 
array window which chunking. Before each hash calculation we will check if the first _config.static.StartLength_ bytes exists 
in the map.

<h3>undedup</h3>
<h4>cache ( lru cache ) </h4>Used in order to optimize undedup performance. After loading a chuck from the 
compressed file we will store it in the cache. The cache size is defined in _config.static.CacheSize_.

<h3>I/O</h3>
<h4>buffer ( *bytes.Buffer ) </h4> All the readers and writers used in project use buffers 
in order to optimize performance and prevent calling syscalls for every I/O command.