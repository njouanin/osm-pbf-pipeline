[![Build Status](https://travis-ci.org/tripod-oss/osm-pbf-pipeline.svg?branch=master)](https://travis-ci.org/tripod-oss/osm-pbf-pipeline)  [ ![Download](https://api.bintray.com/packages/tripod/maven/osm-pbf-pipeline/images/download.svg?version=0.0.2) ](https://bintray.com/tripod/maven/osm-pbf-pipeline/0.0.2/link) 
 
#osm-pbf-pipeline

Scala library for reading Open Street Map [pbf files](http://wiki.openstreetmap.org/wiki/PBF_Format).

osm-pbf-pipeline uses [akka-stream](http://doc.akka.io/docs/akka/2.4/scala/stream/index.html) to provide a 
high-performance, back pressured engine for reading pbf streams.
 
## Quickstart

Add the following dependency to `buid.sbt` (or whatever build system configuration): 

```
libraryDependencies += "io.tripod" %% "osm-pbf-pipeline" % "0.0.2"
```

The see [examples](https://github.com/tripod-oss/osm-pbf-pipeline/tree/master/src/main/scala/io/tripod/pipeline/osm/pbf/examples) for use cases and to learn how to use osm-pbf-pipeline.
