# Spark Project

For the course "Large Scale Data Management Systems".

## Compilation

We open a terminal and we navigate to the folder that contains the vagrant file. There, we run:
```bash
vagrant up
vagrant ssh
hadoop/sbin/start-dfs.sh
spark/sbin/start-all.sh
```
This is the VM terminal.

First, we create a folder into the VM by typing:
```bash
hadoop/bin/hadoop fs -mkdir -p /sql/input
````
We need to put the movies.dat and ratings.dat files into the VM. We do that by running:
```bash
hadoop/bin/hadoop fs -put /vagrant/data/example4/input/movies.dat /sql/input
hadoop/bin/hadoop fs -put /vagrant/data/example4/input/ratings.dat /sql/input
````
We can check that they were put into the VM with the command:
```bash
hadoop/bin/hadoop fs -ls /sql/input/
```
We open another terminal where the pom.xml file is. For example, we will demonstrate how to run the first file, which is "MostRatedRDD.java". We build the project by running:
```bash
mvn clean install
```

The, on the VM terminal, we run:
```bash
spark/bin/spark-submit --class org.hua.Example /vagrant/data/example4/sql/target/sql-0.1.jar hdfs://localhost:54310/sql/input hdfs://localhost:54310/sql/output
```
We will see the console output there.

If we wish to display the contents of the file we type the below command in the VM terminal.
```bash
hadoop/bin/hadoop fs -cat /sql/output/MostRatedRDD/part-*
```
We do the same for the remaining classes.