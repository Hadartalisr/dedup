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
example output - 
``` 
INFO[0034] Process time - 34.757836 seconds.            
INFO[0034] Input File [./input/5000-100] size - 5242880000 Bytes 
INFO[0034] Output File [./output/5000-100] size - 117829784 Bytes 
INFO[0034] Dedup factor - 44.495371                     
INFO[0034] Process speed - 143.852456 MB/Sec            
INFO[0034] ***** Memory Usage *****                     
INFO[0034]      Alloc = 176 MiB                             
INFO[0034]      Sys = 235 MiB                               
INFO[0034]      NumGC = 105    
```
==============================================================
- UnDedupe
``` 
 ./Deduper -undedup ./input-file-name ./output-file-name 
```
example output -
``` 
INFO[0009] Process time - 9.445625 seconds.             
INFO[0009] Input File [./output/5000-100] size - 117829784 Bytes 
INFO[0009] Output File [./output/5000-100-res] size - 5242880000 Bytes 
INFO[0009] Process speed - 529.345575 MB/Sec            
INFO[0009] ***** Memory Usage *****                     
INFO[0009]      Alloc = 391 MiB                             
INFO[0009]      Sys = 1172 MiB                              
INFO[0009]      NumGC = 86         
```
==============================================================
- Compare
``` 
 ./Deduper -compare ./input-file-name ./output-file-name 
```
example output -
``` 
!..Files are equal..!
INFO[0015] Process time - 15.789225 seconds.            
INFO[0015] Input File [./input/5000-100] size - 5242880000 Bytes 
INFO[0015] Output File [./output/5000-100-res] size - 5242880000 Bytes 
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

<h2>Intenral Data Structures</h2>

<h3>dedup</h3>
<h4>hashToOffset (map[uint32]int)</h4>
Used for storing the hash values for the chunks we have already seen before and stored in the compressed file.
The key in the file is a hash of a chunk and the value is its corresponding offset in the compressed file.

<h4>offsets array ( int[] )</h4>
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

<h2>The chunking algorithm </h2>
The chunking used is a variable size chunking.
To find the chunking pattern we have to use a rolling hash.
We define a data window on the data stream which equals _config.static.MinChunkSizeInBytes_ .
</br></br>
Now using a hashing technique we calculate a hash on the window and Check if the hash matches to any preChunked data hash. </br>
__If so__, we add the  preChunked data's offset to the offset array. </br>
__Else__, we slide the window by one element and recalculate the rolling hash until we found a match, 
or we acceded _config.static.MinChunkSizeInBytes_. 
</br></br>
As mentioned before, calculating a hash is an expensive operation in terms of CPU. 
That is the reason we will first chuck if the first first _config.static.StartLength_ bytes exists in the startSet.
If we wanted to improve the startSet elimination FP rate we could add another sets for other indices 
(in the boundary of  _config.static.MinChunkSizeInBytes_) (imitate the idea of a bloom filter for existence check).








