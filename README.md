# CS530 PA3: Hadoop InvertedIndex ReadMe
  Eddie Davis (eddiedavis@u.boisestate.edu)
  Mike Ramshaw (mikeramshaw@u.boisestate.edu)

This is a Hadoop MadReduce implementation of the inverted index algorithm that produces a reverse sorted count of each word in the documents they appear.

# 1) Contents

    src/InvertedIndex.java        Java source code for MapReduce job.
    pom.xml                       Maven configuration file for building the jar file.
    Makefile                      Makefile that invokes Maven (mvn) to build the jar file.
    
# 2) Building

    make
    mvn package
    [INFO] Scanning for projects...
    [INFO]                                                                         
    [INFO] ------------------------------------------------------------------------
    [INFO] Building InvertedIndex 1.0
    [INFO] ------------------------------------------------------------------------
    [INFO] 
    [INFO] --- maven-resources-plugin:2.6:resources (default-resources) @ InvertedIndex ---
    ... 
    [INFO] --- maven-compiler-plugin:3.1:compile (default-compile) @ InvertedIndex ---
    ...
    [INFO] --- maven-resources-plugin:2.6:testResources (default-testResources) @ InvertedIndex ---
    ...
    [INFO] --- maven-compiler-plugin:3.1:testCompile (default-testCompile) @ InvertedIndex ---
    ...
    [INFO] --- maven-surefire-plugin:2.12.4:test (default-test) @ InvertedIndex ---
    ...
    [INFO] --- maven-jar-plugin:3.0.0:jar (default-jar) @ InvertedIndex ---
    [INFO] ------------------------------------------------------------------------
    [INFO] BUILD SUCCESS
    [INFO] ------------------------------------------------------------------------
    [INFO] Total time: 18.834 s
    [INFO] Finished at: 2016-11-17T09:25:16-07:00
    [INFO] Final Memory: 15M/481M
    [INFO] ------------------------------------------------------------------------

# 3) Running

    Executing the jar with no arguments will produce the usage command:

    $ hadoop jar InvertedIndex.jar
    Usage: InvertedIndex <input path> <output path>

    Or to run with inputs (e.g., the etext-all collection):

    $ hadoop jar InvertedIndex.jar inverted-index/etext-all inverted-index/output

Thank you!

