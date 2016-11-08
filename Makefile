all:
	mvn package

clean:
	mvn clean

skip:
	mvn package -DskipTests

venv:
	python vendor/virtualenv-1.11.4/virtualenv.py venv

