//! FastKV Rust async client — usage example.
//!
//! Run:  FASTKV_HOST=localhost FASTKV_PORT=8379 cargo run --example example

use fastkv_client::Client;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut c = Client::connect("127.0.0.1", 8379).await?;

    // Core
    println!("PING  -> {}", c.ping().await?);
    println!("ECHO  -> {}", c.echo("hello fastkv").await?);
    println!("DBSIZE -> {}", c.dbsize().await?);

    // String
    c.set("greeting", "Hello, World!").await?;
    println!("GET greeting -> {:?}", c.get("greeting").await?);

    c.set("counter", "10").await?;
    println!("INCR counter  -> {}", c.incr("counter").await?);
    println!("DECRBY counter 3 -> {}", c.decr_by("counter", 3).await?);

    // Hash
    c.hset("user:1", "name", "Alice").await?;
    c.hset("user:1", "email", "alice@example.com").await?;
    let all = c.hgetall("user:1").await?;
    println!("HGETALL user:1 -> {:?}", all);

    // List
    c.del(&["mylist"]).await?;
    c.rpush("mylist", &["a", "b", "c"]).await?;
    c.lpush("mylist", &["z"]).await?;
    println!("LRANGE mylist -> {:?}", c.lrange("mylist", 0, -1).await?);

    // Pipeline
    let mut p = c.pipeline();
    p.set("p1", "100");
    p.set("p2", "200");
    p.incr("p1");
    p.get("p1");
    let res = p.execute().await?;
    println!("Pipeline INCR -> {}", res.integer(2)?);
    println!("Pipeline GET  -> {:?}", res.string(3)?);

    // Cleanup
    c.del(&["greeting", "counter", "user:1", "mylist", "p1", "p2"]).await?;

    c.close().await;
    println!("\nAll done.");
    Ok(())
}
