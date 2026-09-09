use tokio::net::TcpStream;
use tokio::time::{timeout, Duration, error::Elapsed};
use craftping::tokio::ping;
use craftping::Response;
use sqlx::{Pool, Postgres};
use sqlx::postgres::PgPoolOptions;
use tokio::task::JoinSet;
use serde::{Deserialize, Serialize};
use ipnetwork::IpNetwork;
use serde_json::Value;
use craftping::Player;
use craftping::Error;
#[tokio::main]
async fn main() {
    let pool = PgPoolOptions::new()
        .max_connections(50)
        .connect("postgres://postgres:Erik0601!@mcinfos.de/postgres").await.unwrap();
	loop {
    let ips = db_read(&pool).await;
    let mut set = JoinSet::new();
    for ip in ips {
    set.spawn(async move {
        let ergebnis = timeout(Duration::from_secs(5), pinger(ip)).await;
        let ergebnis = match ergebnis {
            Ok(k) => Some(k),
            _ => None,
        };
    });
    }
    let mut ergebnisse_ip: Vec<String> = Vec::new();
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
                    Some(r) => {
                        match r {
                            Ok(ergebnis) => {
                                let ergebnis = ergebnis.0;
                                ergebnisse_ip.push(ergebnis.1);
                                ergebnisse_version.push(ergebnis.version);
                                ergebnisse_protocol.push(ergebnis.protocol as i32);
                                ergebnisse_max_players.push(ergebnis.max_players as i32);
                                ergebnisse_online_players.push(ergebnis.online_players as i32);
                                ergebnisse_description.push(descriptioniser(ergebnis.description));
                                ergebnisse_sample.push(sampler(ergebnis.sample));
                                }
                            Err(_) => {continue},
                        }
                    },
                    None => {continue},
                }
            },
            Err(e) => {continue;},
        };
    }
    let lenght_test = &ergebnisse_version.len();
    let Result = db_write(&pool, ergebnisse_ip, ergebnisse_version, ergebnisse_protocol, ergebnisse_max_players, ergebnisse_online_players, ergebnisse_description, ergebnisse_sample).await;
    println!("20k fertig! Vec_leage: {lenght_test} Batch-insert Ergebnis: {:?}", Result);
}}
async fn db_read(pool: &Pool<Postgres>)  -> Vec<IpNetwork> {
    let ips: Vec<IpNetwork> = sqlx::query_scalar("SELECT IP FROM IPS LIMIT 200;")
        .fetch_all(pool)
        .await
        .unwrap();
    ips
}
async fn pinger(ip: IpNetwork) -> (Option<Result<Response, std::io::Error>>, String) {
    let hostnamee: &str = &ip.to_string();
    let hostname: &str = "5.175.249.209";
    let port = 25565;
    let stream_nt_unwraped = TcpStream::connect((hostname, port)).await;
    let mut stream = match stream_nt_unwraped {
        Ok(k) => {k},
        Err(e) => {return (Some(Err(e)), ip.to_string());},
    };
    let ping = ping(&mut stream, hostname, port).await.ok();
    return (Some(ping), hostname);
}
async fn db_write(pool: &Pool<Postgres>, ergebnisse_ip: Vec<String>, ergebnisse_version: Vec<String>, ergebnisse_protocol: Vec<i32>, ergebnisse_max_players: Vec<i32>, ergebnisse_online_players: Vec<i32>, ergebnisse_description: Vec<String>, ergebnisse_sample: Vec<String>) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error>{
    let Result = sqlx::query(
        r#"INSERT INTO scanner_ergebnisse (ip, version, protocol, max_players, online_players, sample, description) SELECT * FROM UNNEST($1, $2, $3, $4, $5, $6)"#)
        .bind(ergebnisse_ip)
        .bind(ergebnisse_version)
        .bind(ergebnisse_protocol)
        .bind(ergebnisse_max_players)
        .bind(ergebnisse_online_players)
        .bind(ergebnisse_description)
        .bind(ergebnisse_sample)
        .execute(pool)  
        .await;
    Result
}

async fn as_i32(zahl: &usize) -> i32 {
    let i32_zahl: i32 = *zahl as i32;
    i32_zahl
}
fn sampler(players: Option<Vec<Player>>) -> String {
    let json = serde_json::to_string(&players).unwrap();
    json
}
fn descriptioniser(description: Option<Value>) -> String {
    let json = serde_json::to_string(&description).unwrap();
    json
}
