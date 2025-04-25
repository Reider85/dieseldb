package diesel;
import java.util.*;

interface Query<T> {
    T execute(Table table);
}

