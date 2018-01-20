# Postgres EventStore 

Postgres implementation of an Event Store

Requires Postgres 9.5 or above because: 
 - Uses JSONB columns
 - Uses ON CONFLICT

 
## Installation

Add in your `pom.xml` file the jitpack.io repositories:

```xml
<repositories>
  <repository>
    <id>jitpack.io</id>
    <url>https://jitpack.io</url>
  </repository>
</repositories>
```
  
Now add the package as a dependency: 

```xml
<dependencies>		
  <dependency>
    <groupId>com.github.com-nilportugues</groupId>
    <artifactId>eventstore-postgres</artifactId>
    <version>${eventstore-postgres.version}</version>
  </dependency>
</dependencies>  
```

## Authors

* [Nil Portugués Calderó](https://nilportugues.com) (contact@nilportugues.com)


## License
The code base is licensed under the [MIT license](LICENSE).

