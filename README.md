# nifi-script-tester
A project to create a stub/mock environment for testing ExecuteScript processors.

## Usage

java -cp nifi-script-tester-<version>-all.jar [options] script_file

  Where options may include:
  
    -success            Output information about flow files that were transferred to the success relationship. Defaults to true
    
    -failure            Output information about flow files that were transferred to the failure relationship. Defaults to false
    
    -no-success         Do not output information about flow files that were transferred to the success relationship. Defaults to false
    
    -content            Output flow file contents. Defaults to false
    
    -attrs              Output flow file attributes. Defaults to false
    
    -all-rels           Output information about flow files that were transferred to any relationship. Defaults to false
    
    -all                Output content, attributes, etc. about flow files that were transferred to any relationship. Defaults to false
    
    -input=<directory>  Send each file in the specified directory as a flow file to the script
    
    -modules=<paths>    Comma-separated list of paths (files or directories) containing script modules/JARs
    
    
    
## Build

To build the fat JAR, just run the following command:

```gradle
gradle shadowJar
```


## Download
The JAR is available on Bintray at https://bintray.com/mattyb149/maven/nifi-script-tester


### Maven
```maven
<dependency>
  <groupId>mattyb149</groupId>
  <artifactId>nifi-script-tester</artifactId>
  <version>1.1.1</version>
  <type>jar</type>
  <classifier>all</classifier>
</dependency>
```

### Gradle
```gradle
compile(group: 'mattyb149', name: 'nifi-script-tester', version: '1.1.1', ext: 'jar', classifier: 'all')
```

## License

nifi-script-tester is copyright 2016- Matthew Burgess except where otherwise noted.

This project is licensed under the Apache License Version 2.0 except where otherwise noted in the source files.
