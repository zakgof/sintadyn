# Sintadyn

**Si**ngle-**ta**ble **Dyn**amoDB design Java mapper.

As a typical NoSQL database, DynamoDB does not support table joins and other relational database constructs. 
However, you can model many common relational designs in a single DynamoDB table.
Normally this requires some well-known tricks with table keys and GSI. 

**Sintadyn** handles all those details internally, providing high-level API for storing Java objects and their relationships.

## Quick-start

#### Create a Sintadyn instance
````java
Sintadyn sintadyn = Sintadyn.create();
````
#### Create a single DynamoDB table for everything
````java
DynamoDbClient dynamoDB = DynamoDbClient.create();
sintadyn.createTable(dynamoDB);
````

#### Example model

A MOVIE node is connected to ACTOR nodes via ROLE edges.
````
[MOVIE] -------[ROLE]-------> [ACTOR] 
````

#### Define node and edge classes. Nodes have keys:
````java
public class Movie {
    @Key
    private String id;
    private String title;
    ...
}

public class Actor {
    @Key
    private String id;
    private String name;
    ...
}

public class Role {
    private String characterName;
    private int salary;
}

````
#### Define Sintadyn node
````java
Node<String, Movie> movieNode = Sintadyn.node(Movie.class);
````

#### CRUD operations on nodes
````java
Movie movie = new Movie(...);

movieNode.put()
        .value(movie)
        .execute(sintadyn, dynamoDB);

Order order1 = orderType.get()
        .key("12-34-56")
        .execute(sintadyn, dynamoDB);

orderType.delete()
        .key("12-34-56")
        .execute(sintadyn, dynamoDB);
````
#### One-to-Many relationship
