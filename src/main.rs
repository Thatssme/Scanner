use tokio::{task::JoinSet, task, net::TcpStream, time::{timeout, Duration}};
use craftping::tokio::ping;
use sqlx::{Pool, Postgres, QueryBuilder};
use sqlx::postgres::PgPoolOptions;
use ipnetwork::IpNetwork;
use serde_json::Value;
use serde::Serialize;
use craftping::Player;
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, Semaphore, RwLockWriteGuard};
use std::io;
#[derive(PartialEq, Debug, Clone, sqlx::FromRow)]
struct IpInfo {
    ip: IpNetwork,
    port: i16,
    version: String,
    protocol: i16,
    max_players: i32,
    #[sqlx(default)]
    online_players: Option<i32>,
    description: Option<String>,
    #[sqlx(default)]
    sample: Option<Vec<String>>,
}
struct Update {
    ip: IpNetwork,
    port: i16,
    online_players: Option<i32>,
    sample: Option<Vec<String>>, 
}
trait Json: Serialize {}
impl Json for Vec<Player> {}
impl Json for Value {}
#[tokio::main]
async fn main() {
    let pool = PgPoolOptions::new()
        .max_connections(50)
        .connect("postgres://postgres:Erik0601!@5.175.249.209/postgres").await.unwrap();
    println!("DB verbunden");
    let mut ergebnisse: Vec<IpInfo> = Vec::new();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<IpInfo>(10000);
    let len: Arc<RwLock<i32>> = Arc::new(RwLock::new(10000));
    let scans: Arc<RwLock<HashMap<IpNetwork, Option<IpInfo>>>> = db_read_ips(&pool).await;
    let pool_cloned = pool.clone();
    let len_clone = Arc::clone(&len);
    let scans_anders = Arc::clone(&scans);
    task::spawn(async move {
        loop {
        let guard = len_clone.read().await;
        let len = *guard;
        drop(guard);
        while let Some(infos) = rx.recv().await {
            ergebnisse.push(infos);
            if len != 10000 {
                let guard = scans_anders.write().await;
                let _ = db_write(&pool_cloned, &ergebnisse, guard).await;
                continue;
            }
            if ergebnisse.len() >= 100 {
                let guard = scans_anders.write().await;
                let ergebnis = std::mem::take(&mut ergebnisse);
                let _ = db_write(&pool_cloned, &ergebnis, guard).await;
                continue;
            }
        }}
    });
    let mut index: i64 = 0;
    let mut scans_pro_sek = String::new();
    io::stdin()
        .read_line(&mut scans_pro_sek)
        .expect("Machs richtig");
    let sem = Arc::new(Semaphore::new(scans_pro_sek.trim().parse().unwrap()));
    let mut anzahl = 0;
    loop {
    let ips = db_read(&pool, &index).await;
    let mut guard = len.write().await;
    *guard = ips.len() as i32;
    drop(guard);
    let mut set = JoinSet::new();
    println!("Anzahl: {}, Ips gelesen anzahl sollten 10k sein: {}", anzahl, ips.len());
    anzahl += 10000;
    for ip in ips {
    let sem = Arc::clone(&sem);
    let scan = Arc::clone(&scans);
    let tx = tx.clone();
    let _permit = sem.clone().acquire_owned().await.unwrap();
    set.spawn(async move {
        let _erlaubnis = sem.acquire().await.unwrap();
        let guard = scan.read().await;
        if let Some(None) = guard.get(&ip) {
            println!("Opt-out");
            return;
        }
        drop(guard);
        let ergebnis = timeout(Duration::from_secs(4), pinger(ip)).await;
        let sendung = match ergebnis {
            Ok(k) => {match k {
                Some(s) => {s},
                None => return,
            }}
            _ => {return}
        };
        let guard = scan.read().await;
        if guard.contains_key(&ip) {
            if Some(&Some(sendung.clone())) == guard.get(&ip) {
            return;
        }        
        }
        let _ = tx.send(sendung).await;
    });
    }
    while let Some(s) = set.join_next().await {
        if let Err(e) = s {
            eprintln!("{:?}", e)
        }
    }
    index += 10000;
}
}
async fn db_read(pool: &Pool<Postgres>, index: &i64)  -> Vec<IpNetwork> {
    let ips: Vec<IpNetwork> = sqlx::query_scalar("SELECT ip, id FROM ips WHERE id > $1 ORDER BY id LIMIT 10000;")
        .bind(index)
        .fetch_all(pool)
        .await
        .unwrap();
    ips
}
async fn pinger(ip: IpNetwork) -> Option<IpInfo> {
    let hostname: &str = &ip.ip().to_string();
    let port: u16 = 25565;
    let stream = TcpStream::connect((hostname, port)).await.ok();
    if let Some(mut stream) = stream {
    let ping_fehler = ping(&mut stream, hostname, port).await;
    let ping = match ping_fehler {
        Ok(k) => Some(k),
        Err(_) => {None}
    };
    match ping {
        Some(ping) => {
            let ergebnis = IpInfo {
                        ip: hostname.parse::<IpNetwork>().unwrap(),
                        port: port as i16,
                        version: ping.version,
                        protocol: ping.protocol as i16,
                        max_players: ping.max_players as i32,
                        online_players: Some(ping.online_players as i32),
                        description: jsoniser(ping.description),
                        sample: ping.sample.map(|players| {players.iter().map(|player| player.name.clone()).collect()}),
                };
                println!("Server gefunden Ip: {}", ergebnis.ip);
            return Some(ergebnis);
        },
        _ => {return None},
    }
}
return None;
}
async fn db_write(pool: &Pool<Postgres>, ergebnisse1: &Vec<IpInfo>,mut guard: RwLockWriteGuard<'_, HashMap<IpNetwork, Option<IpInfo>>>) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    let ergebnisse = ergebnisse1.clone();
    let mut ergebnisse_ip: Vec<IpNetwork> = Vec::with_capacity(10000);
    let mut ergebnisse_version: Vec<String> = Vec::with_capacity(10000);
    let mut ergebnisse_protocol: Vec<i16> = Vec::with_capacity(10000);
    let mut ergebnisse_max_players: Vec<i32> = Vec::with_capacity(10000);
    let mut ergebnisse_online_players: Vec<i32> = Vec::with_capacity(10000);
    let mut ergebnisse_sample: Vec<Option<Vec<String>>> = Vec::with_capacity(10000);
    let mut ergebnisse_description: Vec<Option<String>> = Vec::with_capacity(10000);
    let mut bereits_gescanned: Vec<Update> = Vec::with_capacity(10000);
    for e in ergebnisse {
        if let Some(Some(i)) = guard.get(&e.ip).clone() {
            if i.version == e.version
                &&i.protocol == e.protocol
                &&i.max_players == e.max_players
                ||i.description == e.description {
                    if i.sample != e.sample
                    &&i.online_players != e.online_players {
                    let update = Update {
                        ip: e.ip,
                        port: e.port,
                        online_players: e.online_players.clone(),
                        sample: e.sample.clone(),
                    };
                    guard.insert(e.ip, Some(e.clone()));
                    bereits_gescanned.push(update);   
                    }
                }
            continue;
        }
        ergebnisse_ip.push(e.ip);
        ergebnisse_version.push(e.version);
        ergebnisse_protocol.push(e.protocol);
        ergebnisse_max_players.push(e.max_players);
        ergebnisse_online_players.push(e.online_players.unwrap());
        ergebnisse_sample.push(e.sample);
        ergebnisse_description.push(e.description);
    }
    drop(guard);
    let result = sqlx::query(
        r#"INSERT INTO servers (ip, version, protocol, max_players, description) SELECT * FROM UNNEST($1, $2, $3, $4, $5)"#)
        .bind(ergebnisse_ip)
        .bind(ergebnisse_version)
        .bind(ergebnisse_protocol)
        .bind(ergebnisse_max_players)
        .bind(ergebnisse_description)
        .execute(pool)
        .await;
    if !bereits_gescanned.is_empty() {
    let mut builder = QueryBuilder::<Postgres>::new("INSERT INTO status (ip, port, online_players, sample) ");
    builder.push_values(bereits_gescanned, |mut b, updates| {
        b.push_bind(updates.ip)
         .push_bind(updates.port)
         .push_bind(updates.online_players)
         .push_bind(updates.sample);
    });
    let query = builder.build();
    let _ = query.execute(pool).await;
    }
    result
}
fn jsoniser<T: Json>(players: Option<T>) -> Option<String> {
    if let Some(wert) = players {
        let string = serde_json::to_string(&wert).unwrap_or("Das sollte nicht so sein irgentwas beim jsoniser ist fehlgeschlagen".to_string());
        return Some(string);
    }
    return None;
}
async fn db_read_opt_out(pool: &Pool<Postgres>)  -> Arc<RwLock<HashMap<IpNetwork, Option<IpInfo>>>> {
    let ips: Vec<IpNetwork> = sqlx::query_scalar("SELECT * FROM opt_out")
        .fetch_all(pool)
        .await
        .unwrap();
    let scans: Arc<RwLock<HashMap<IpNetwork, Option<IpInfo>>>> = Arc::new(RwLock::new(HashMap::new()));
    for ip in ips {
        let mut guard = scans.write().await;
        guard.insert(ip, None);
    }
    scans
}
async fn db_read_ips(pool: &Pool<Postgres>)  -> Arc<RwLock<HashMap<IpNetwork, Option<IpInfo>>>> {
    let scans: Arc<RwLock<HashMap<IpNetwork, Option<IpInfo>>>> = db_read_opt_out(pool).await;
    let ips: Vec<IpInfo> = sqlx::query_as::<sqlx::Postgres, IpInfo>("SELECT * FROM servers")
        .fetch_all(pool)
        .await
        .unwrap();
    for ip in ips {
        let mut guard = scans.write().await;
        let ergebnis= IpInfo {
            ip: ip.ip,
            port: ip.port,
            version: ip.version,
            protocol: ip.protocol,
            max_players: ip.max_players,
            online_players: None,
            sample: None,
            description: ip.description,
        };
        guard.insert(ip.ip, Some(ergebnis));
    }
    scans
}

