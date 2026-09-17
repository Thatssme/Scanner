use tokio::{task::JoinSet, net::TcpStream, time::{timeout, Duration}};
use craftping::tokio::ping;
use sqlx::{Pool, Postgres};
use sqlx::postgres::PgPoolOptions;
use ipnetwork::IpNetwork;
use serde_json::Value;
use serde::Serialize;
use craftping::Player;
use std::sync::Arc;
use tokio::sync::Semaphore;
struct IpInfo {
    ip: IpNetwork,
    version: String,
    protocol: i32,
    max_players: i32,
    online_players: i32,
    description: String,
    sample: String,
}
trait Json: Serialize {}
impl Json for Option<Vec<Player>> {}
impl Json for Option<Value> {}
#[tokio::main]
async fn main() {
    let pool = PgPoolOptions::new()
        .max_connections(50)
        .connect("postgres://postgres:Erik0601!@5.175.249.209/postgres").await.unwrap();
    println!("DB verbunden");
    let mut index: i64 = 0;
    let mut ergebnisse: Vec<IpInfo> = Vec::new();
    let sem = Arc::new(Semaphore::new(4000));
    loop {
    let ips = db_read(&pool, &index).await;
    let mut set = JoinSet::new();
    let leange = if ips.len() == 10000 {
        2500
    }
    else {
        ips.len()
    };
    for ip in ips {
    let sem = Arc::clone(&sem);
    set.spawn(async move {
        let _erlaubnis = sem.acquire().await.unwrap();
        let ergebnis = timeout(Duration::from_secs(1), pinger(ip)).await;
        let sendung = match ergebnis {
            Ok(k) => {match k {
                Some(s) => {s},
                None => return None,
            }}
            _ => {return None}
        };
        return Some(sendung);
    });
    }
    while let Some(ergebnis) = set.join_next().await {
        if let Ok(Some(k)) = ergebnis {
                ergebnisse.push(k);
                if ergebnisse.len() >= leange {
                    let batch = std::mem::take(&mut ergebnisse);
                    let _ = db_write(&pool, batch).await;
                    println!("Schreiben beendet. Leange: {leange}");
                }
    }
}
    index += 10000;
}
}
async fn db_read(pool: &Pool<Postgres>, index: &i64)  -> Vec<IpNetwork> {
    let ips: Vec<IpNetwork> = sqlx::query_scalar("SELECT ip FROM ips ORDER BY ip LIMIT 10000 OFFSET $1;")
        .bind(index)
        .fetch_all(pool)
        .await
        .unwrap();
    ips
}
async fn pinger(ip: IpNetwork) -> Option<IpInfo> {
    let hostname: &str = "127.0.0.1";
    let _hostnamee: &str = &ip.ip().to_string();
    
    let port = 25565;
    let stream_nt_unwraped = TcpStream::connect((hostname, port)).await;
    let stream = match stream_nt_unwraped {
        Ok(k) => {Some(k)},
        Err(_) => {return None;},
    };
    if let Some(mut stream) = stream {
    let ping = ping(&mut stream, hostname, port).await.ok();
    match ping {
        Some(ping) => {
            let ergebnis = IpInfo {
                        ip: hostname.parse::<IpNetwork>().unwrap(),
                        version: ping.version,
                        protocol: ping.protocol,
                        max_players: ping.max_players as i32,
                        online_players: ping.online_players as i32,
                        description: jsoniser(ping.description),
                        sample: jsoniser(ping.sample),
                };
            return Some(ergebnis);
        },
        _ => {return None},
    }
}
return None;
}
async fn db_write(pool: &Pool<Postgres>, ergebnisse: Vec<IpInfo>) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    let mut ergebnisse_ip: Vec<IpNetwork> = Vec::new();
    let mut ergebnisse_version: Vec<String> = Vec::new();
    let mut ergebnisse_protocol: Vec<i32> = Vec::new();
    let mut ergebnisse_max_players: Vec<i32> = Vec::new();
    let mut ergebnisse_online_players: Vec<i32> = Vec::new();
    let mut ergebnisse_sample: Vec<String> = Vec::new();
    let mut ergebnisse_description: Vec<String> = Vec::new();
    for e in ergebnisse {
        ergebnisse_ip.push(e.ip);
        ergebnisse_version.push(e.version);
        ergebnisse_protocol.push(e.protocol);
        ergebnisse_max_players.push(e.max_players);
        ergebnisse_online_players.push(e.online_players);
        ergebnisse_sample.push(e.sample);
        ergebnisse_description.push(e.description);
    }
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
fn jsoniser<T: Json>(players: T) -> String {
    let json = serde_json::to_string(&players).unwrap();
    json
}
