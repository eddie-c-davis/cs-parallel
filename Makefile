all:
	mvn package
	mv ./target/InvertedIndex-1.0.jar ./InvertedIndex.jar	

clean:
	mvn clean

