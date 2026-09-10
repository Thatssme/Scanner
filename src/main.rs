use tokio::net::TcpStream;
use tokio::time::{timeout, Duration};
use craftping::tokio::ping;
use craftping::Response;
use sqlx::{Pool, Postgres};
use sqlx::postgres::PgPoolOptions;
use tokio::task::JoinSet;
use ipnetwork::IpNetwork;
use serde_json::Value;
use craftping::Player;
#[tokio::main]
async fn main() {
    let pool = PgPoolOptions::new()
        .max_connections(50)
        .connect("postgres://postgres:Erik0601!@5.175.249.209/postgres").await.unwrap();
    println!("DB verbunden");
    let mut index: i64 = 0;
    loop {
    let ips = db_read(&pool, &index).await;
    let mut set = JoinSet::new();
    for ip in ips {
    set.spawn(async move {
        let ergebnis = timeout(Duration::from_secs(1), pinger(ip)).await;
        match ergebnis {
            Ok((Some(ping), Some(ip))) => return Some((ping, ip)),
            _ => None,
        }
    });
    }
    let mut ergebnisse_ip = Vec::new();
    let mut ergebnisse_version: Vec<String> = Vec::new();
    let mut ergebnisse_protocol: Vec<i32> = Vec::new();
    let mut ergebnisse_max_players: Vec<i32> = Vec::new();
    let mut ergebnisse_online_players: Vec<i32> = Vec::new();
    let mut ergebnisse_description: Vec<String> = Vec::new();
    let mut ergebnisse_sample: Vec<String> = Vec::new();
    while let Some(res1) = set.join_next().await {
        match res1 {
            Ok(res) => {
                match res {
                    Some(ergebnisse) => {
                                let ergebnis = ergebnisse.0;
                                ergebnisse_ip.push(ergebnisse.1.parse::<IpNetwork>().unwrap());
                                ergebnisse_version.push(ergebnis.version);
                                ergebnisse_protocol.push(ergebnis.protocol as i32);
                                ergebnisse_max_players.push(ergebnis.max_players as i32);
                                ergebnisse_online_players.push(ergebnis.online_players as i32);
                                ergebnisse_description.push(descriptioniser(ergebnis.description));
                                ergebnisse_sample.push(sampler(ergebnis.sample));
                    },
                    None => {continue},
                }
            },
            Err(_) => {continue;},
        };
    }
    let lenght_test = &ergebnisse_version.len();
    let result = db_write(&pool, ergebnisse_ip, ergebnisse_version, ergebnisse_protocol, ergebnisse_max_players, ergebnisse_online_players, ergebnisse_description, ergebnisse_sample).await;
    println!("4k fertig! Vec_leange: {lenght_test} Batch-insert Ergebnis: {:?}", result);
    index += 10000;
}
}
async fn db_read(pool: &Pool<Postgres>, index: &i64)  -> Vec<IpNetwork> {
    let ips: Vec<IpNetwork> = sqlx::query_scalar("SELECT ip FROM ips LIMIT 10000 OFFSET $1;")
        .bind(index)
        .fetch_all(pool)
        .await
        .unwrap();
    ips
}
async fn pinger(ip: IpNetwork) -> (Option<Response>, Option<String>) {
    let hostname: &str = &ip.ip().to_string();
    let port = 25565;
    let stream_nt_unwraped = TcpStream::connect((hostname, port)).await;
    let stream = match stream_nt_unwraped {
        Ok(k) => {Some(k)},
        Err(_) => {return (None, None);},
    };
    if let Some(mut stream) = stream {
    let ping = ping(&mut stream, hostname, port).await.ok(); 
        return (ping, Some(hostname.to_string()));
    }
    return (None, None);
}
async fn db_write(pool: &Pool<Postgres>, ergebnisse_ip: Vec<IpNetwork>, ergebnisse_version: Vec<String>, ergebnisse_protocol: Vec<i32>, ergebnisse_max_players: Vec<i32>, ergebnisse_online_players: Vec<i32>, ergebnisse_description: Vec<String>, ergebnisse_sample: Vec<String>) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    let result = sqlx::query(
        r#"INSERT INTO scanner_ergebnisse (ip, version, protocol, max_players, online_players, sample, description) SELECT * FROM UNNEST($1, $2, $3, $4, $5, $6, $7)"#)
        .bind(ergebnisse_ip)
        .bind(ergebnisse_version)
        .bind(ergebnisse_protocol)
        .bind(ergebnisse_max_players)
        .bind(ergebnisse_online_players)
        .bind(ergebnisse_sample)
        .bind(ergebnisse_description)
        .execute(pool)  
        .await;
    result
}
fn sampler(players: Option<Vec<Player>>) -> String {
    let json = serde_json::to_string(&players).unwrap();
    json
}
fn descriptioniser(description: Option<Value>) -> String {
    let json = serde_json::to_string(&description).unwrap();
    json
}
