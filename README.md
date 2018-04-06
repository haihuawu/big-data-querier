# big-data-querier
Given a data set collection, design and implement a query  interface that helps users compare columns and sets of value. For example, the interface should support the following queries:(1) What are the columns that values x and y both occur in (and z does not occur in). This type of query returns a set of column ids. (2) Given a set of column ids what (a) is the intersection of the values in the columns, (b) are the most frequent values in the column set (where frequency is the number of columns a value occurs in).

## thanks to big god [spoddutur](https://github.com/spoddutur/spark-as-service-using-embedded-server) for the infrastructure

## spark-as-service-using-embedded-server
This application comes as Spark2.1-REST-Service-Provider using an embedded, Reactive-Streams-based, fully asynchronous HTTP server.

## Following picture illustrates the routing of a HttpRequest:
<img src="https://user-images.githubusercontent.com/22542670/27865894-ee70d42a-61b1-11e7-9595-02b845a9ffae.png" width="700"/>


## Setting up Hadoop on local machine
* use [tutorial](https://dtflaneur.wordpress.com/2015/10/02/installing-hadoop-on-mac-osx-el-capitan/)
* make sure hadoop core-site.xml, set url to ```hdfs://localhost:9000```, because it is set up as the hdfs address in this project in ```AppConfig.scala```


## Building
It uses [Scala 2.11](#scala), [Spark 2.1](#spark) and [Akka-Http](#akka-http)
```markdown
mvn clean install
```
## Execution
We can start our application as stand-alone jar like this:
```markdown
mvn exec:java
```