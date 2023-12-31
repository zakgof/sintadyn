package com.zakgof.sintadyn;

import com.zakgof.sintadyn.mockdyn.MockDynamoDbClient;
import com.zakgof.sintadyn.model.Book;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class CrudTest {

    private static final MockDynamoDbClient dynamoDB = new MockDynamoDbClient();
    private static final Node<String, Book> BOOK_NODE = Sintadyn.node(Book.class);
    private static final Sintadyn sintadyn = Sintadyn.builder().build();

    private static final Book QUIXOTE = new Book("9783161484100", "Don Quixote");
    private static final Book HOBBIT = new Book("9780008376055", "The Hobbit");
    private static final Book KOBZAR = new Book("9789668182563", "Kobzar");

    private static final Book QUIXOTE2 = new Book("9783161484100", "Don Quixote Updated");
    private static final Book HOBBIT2 = new Book("9780008376055", "The Hobbit Updated");
    private static final Book KOBZAR2 = new Book("9789668182563", "Kobzar Updated");


    @BeforeEach
    void setup() {

        sintadyn.createTable(dynamoDB);

        BOOK_NODE.put()
                .values(List.of(QUIXOTE, HOBBIT))
                .execute(sintadyn, dynamoDB);

        assertBooks(QUIXOTE, HOBBIT);
    }

    @Test
    void create() {
        BOOK_NODE.put()
                .value(KOBZAR)
                .execute(sintadyn, dynamoDB);

        assertBooks(QUIXOTE, HOBBIT, KOBZAR);
    }

    @Test
    void createBatch() {
        BOOK_NODE.put()
                .values(List.of(HOBBIT2, KOBZAR))
                .execute(sintadyn, dynamoDB);

        assertBooks(QUIXOTE, HOBBIT2, KOBZAR);
    }


    @Test
    void read() {
        Book book = BOOK_NODE.get()
                .key(HOBBIT.getIsbn())
                .execute(sintadyn, dynamoDB);

        assertThat(book).isEqualTo(HOBBIT);
    }

    @Test
    void readBatch() {
        List<Book> books = BOOK_NODE.get()
                .keys(List.of(HOBBIT.getIsbn(), QUIXOTE.getIsbn()))
                .execute(sintadyn, dynamoDB);

        assertThat(books).isEqualTo(List.of(HOBBIT, QUIXOTE));
    }

    @Test
    void readAll() {
        List<Book> books = BOOK_NODE.get()
                .all()
                .execute(sintadyn, dynamoDB);

        assertThat(books).isEqualTo(List.of(HOBBIT, QUIXOTE));
    }

    @Test
    void update() {
        BOOK_NODE.put()
                .value(HOBBIT2)
                .execute(sintadyn, dynamoDB);

        assertBooks(QUIXOTE, HOBBIT2);
    }

    @Test
    void delete() {
        BOOK_NODE.delete()
                .key(HOBBIT.getIsbn())
                .execute(sintadyn, dynamoDB);

        assertBooks(QUIXOTE);
    }

    @Test
    void deleteBatch() {
        BOOK_NODE.delete()
                .keys(List.of(HOBBIT.getIsbn(), QUIXOTE.getIsbn()))
                .execute(sintadyn, dynamoDB);

        assertBooks();
    }

    private void assertBooks(Book... books) {
        Set<Book> actualBooks = new HashSet<>(BOOK_NODE.get()
                .keys(List.of(QUIXOTE.getIsbn(), HOBBIT.getIsbn(), KOBZAR.getIsbn()))
                .execute(sintadyn, dynamoDB));

        assertThat(actualBooks).isEqualTo(Set.of(books));
    }
}
