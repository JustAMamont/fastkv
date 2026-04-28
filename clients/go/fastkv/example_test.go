package fastkv_test

import (
        "fmt"
        "log"

        "github.com/JustAMamont/fastkv"
)
// Example_basicSetGet demonstrates basic SET and GET operations.
func Example_basicSetGet() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        if err := client.Set("greeting", "hello, fastkv"); err != nil {
                log.Fatal(err)
        }

        val, err := client.Get("greeting")
        if err != nil {
                log.Fatal(err)
        }
        fmt.Println(val)

        // Output: hello, fastkv
}

// Example_basicSetGet_nonexistent demonstrates that GET on a missing key
// returns ErrNil.
func Example_basicSetGet_nonexistent() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        _, err := client.Get("nonexistent")
        fmt.Println(err)

        // Output: fastkv: nil reply
}

// Example_basicSetGet_del demonstrates DEL returning the number of keys removed.
func Example_basicSetGet_del() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        _ = client.Set("temp", "bye")
        n, err := client.Del("temp")
        if err != nil {
                log.Fatal(err)
        }
        fmt.Println(n)

        // Output: 1
}

// Example_basicSetGet_setNX demonstrates SET NX (only-if-not-exists).
func Example_basicSetGet_setNX() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        _ = client.Set("key", "original")
        ok, err := client.SetNX("key", "will not overwrite")
        if err != nil {
                log.Fatal(err)
        }
        fmt.Println(ok)

        // Output: false
}

// Example_integerOperations shows INCR / INCRBY / DECRBY usage.
func Example_integerOperations() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        _ = client.Set("counter", "10")

        v, _ := client.Incr("counter")
        v, _ = client.IncrBy("counter", 5)
        v, _ = client.DecrBy("counter", 3)
        fmt.Println(v)

        // Output: 13
}

// Example_ttlOperations demonstrates Expire, Ttl, and Persist.
func Example_ttlOperations() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        _ = client.Set("temp", "I expire")
        ok, _ := client.Expire("temp", 60)
        _ = ok // true

        // Remove the TTL — key becomes persistent again.
        ok, err := client.Persist("temp")
        if err != nil {
                log.Fatal(err)
        }
        fmt.Println(ok)

        // Output: true
}

// Example_hashOperations demonstrates hash (HSET, HGET, HGETALL, HMSET, HMGet).
func Example_hashOperations() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        _, _ = client.HSet("user:1", "name", "Alice")
        _, _ = client.HSet("user:1", "email", "alice@example.com")

        all, err := client.HGetAll("user:1")
        if err != nil {
                log.Fatal(err)
        }
        fmt.Println(all["name"], all["email"])

        // Output: Alice alice@example.com
}

// Example_hashOperations_hmset demonstrates HMSet and HMGet.
func Example_hashOperations_hmset() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        _ = client.HMSet("user:2", map[string]string{
                "name":  "Bob",
                "email": "bob@example.com",
        })

        vals, err := client.HMGet("user:2", "name", "email")
        if err != nil {
                log.Fatal(err)
        }
        fmt.Println(vals[0], vals[1])

        // Output: Bob bob@example.com
}

// Example_listOperations demonstrates list push, pop, range, and trim.
func Example_listOperations() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()
        defer client.Del("queue")

        client.Del("queue")
        client.RPush("queue", "task1", "task2", "task3")
        client.LPush("queue", "urgent")

        items, _ := client.LRange("queue", 0, -1)
        fmt.Println(items)

        // Output: [urgent task1 task2 task3]
}

// Example_listOperations_pop demonstrates LPop and RPop.
func Example_listOperations_pop() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        client.Del("queue")
        client.RPush("queue", "a", "b", "c")
        head, _ := client.LPop("queue")
        tail, _ := client.RPop("queue")
        fmt.Println(head, tail)

        // Output: a c
}

// Example_listOperations_trim demonstrates LPush + LTrim.
func Example_listOperations_trim() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        client.Del("queue")
        _, _ = client.RPush("queue", "task1", "task2")
        _, _ = client.LPush("queue", "urgent")
        _ = client.LTrim("queue", 0, 1)

        items, _ := client.LRange("queue", 0, -1)
        fmt.Println(items)

        // Output: [urgent task1]
}

// Example_pipeline demonstrates batching commands with Pipeline.
func Example_pipeline() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        p := client.Pipeline()
        p.Set("p:a", "1")
        p.Set("p:b", "2")
        p.Incr("p:a")
        p.Get("p:a")

        res, err := p.Execute()
        if err != nil {
                log.Fatal(err)
        }

        val, _ := res.String(3)
        fmt.Println(val)

        // Output: 2
}

// Example_pipeline_mget demonstrates MGet inside a pipeline.
func Example_pipeline_mget() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        p := client.Pipeline()
        p.Set("x", "10")
        p.Set("y", "20")
        p.MGet("x", "y", "z")

        res, _ := p.Execute()
        vals, _ := res.StringSlice(2)
        fmt.Println(vals)

        // Output: [10 20 ]
}

// Example_msetMget demonstrates MSET and MGET for batch operations.
func Example_msetMget() {
        client := fastkv.NewClient("localhost:8379")
        defer client.Close()

        _ = client.MSet(map[string]string{
                "k1": "v1",
                "k2": "v2",
                "k3": "v3",
        })

        vals, _ := client.MGet("k1", "k2", "k3", "missing")
        fmt.Println(vals)

        // Output: [v1 v2 v3 ]
}
