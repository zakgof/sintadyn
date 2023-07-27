package com.zakgof.sintadyn;

import com.zakgof.sintadyn.model.Actor;
import com.zakgof.sintadyn.model.Movie;
import com.zakgof.sintadyn.model.Role;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.ProfileCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

class ManyToManyTest {

    private static final DynamoDbClient dynamoDB = DynamoDbClient.builder()
            .credentialsProvider(ProfileCredentialsProvider.create("sintadyn"))
            .region(Region.US_EAST_1)
            .build();

    // new MockDynamoDbClient();
    private static final Node<String, Movie> MOVIE_NODE = Sintadyn.node(Movie.class);
    private static final Node<String, Actor> ACTOR_NODE = Sintadyn.node(Actor.class);
    private static final Sintadyn sintadyn = Sintadyn.builder().build();

    private static final Movie TAXI_DRIVER = new Movie("00001", "Taxi Driver", 1976);
    private static final Movie GOODFELLAS = new Movie("00002", "Goodfellas", 1990);

    private static final Role TRAVIS = new Role("Travis Bickle");
    private static final Role JIMMY = new Role("Jimmy Conway");
    private static final Role HENRY = new Role("Henry Hill");

    private static final Actor DE_NIRO = new Actor("A00001", "Robert De Niro", 1943);
    private static final Actor LIOTTA = new Actor("A00002", "Ray Liotta", 1954);

    private static final ManyToMany<String, Movie, String, Actor, Role> ROLES = Sintadyn.manyToMany(MOVIE_NODE, ACTOR_NODE, Role.class);

    @BeforeEach
    void setup() {

        // sintadyn.createTable(dynamoDB);

        MOVIE_NODE.put()
                .values(List.of(TAXI_DRIVER, GOODFELLAS))
                .execute(sintadyn, dynamoDB);
        ACTOR_NODE.put()
                .values(List.of(DE_NIRO, LIOTTA))
                .execute(sintadyn, dynamoDB);
    }

    @Test
    void put() {
        ROLES.connect()
                .fromValue(GOODFELLAS)
                .withEdge(JIMMY)
                .toValue(DE_NIRO)
                .execute(sintadyn, dynamoDB);
        ROLES.connect()
                .fromValue(GOODFELLAS)
                .withEdge(HENRY)
                .toValue(LIOTTA)
                .execute(sintadyn, dynamoDB);

        List<Role> roles = ROLES.getEdges()
                .value(GOODFELLAS)
                .execute(sintadyn, dynamoDB);

        assertThat(roles).hasSize(2);
        assertThat(new HashSet<>(roles)).isEqualTo(Set.of(JIMMY, HENRY));

        List<Link<String, Actor, Role>> links = ROLES.get()
                .value(GOODFELLAS)
                .execute(sintadyn, dynamoDB);

        assertThat(links).hasSize(2);
        assertThat(links.stream().map(Link::edge).collect(Collectors.toSet())).isEqualTo(Set.of(JIMMY, HENRY));
        assertThat(links.stream().map(Link::node).collect(Collectors.toSet())).isEqualTo(Set.of(DE_NIRO, LIOTTA));

    }

}
