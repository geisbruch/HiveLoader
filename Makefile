clean:
	rm -fr ml-build/hiveLoader
	mkdir -p ml-build/hiveLoader

meli-test:

meli-build: clean
	mvn -f hiveLoader/pom.xml assembly:assembly
	cp -fr hiveLoader/target/lib ml-build/hiveLoader/
	cp -fr hiveLoader/target/*assembly.jar ml-build/hiveLoader/
