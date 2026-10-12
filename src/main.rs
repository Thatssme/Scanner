use tokio::{task::JoinSet, task, net::TcpStream, time::{timeout, Duration}};
use craftping::tokio::ping;
use sqlx::{Pool, Postgres, QueryBuilder};
use sqlx::postgres::PgPoolOptions;
use ipnetwork::IpNetwork;
use serde_json::Value;
use serde::Serialize;
use craftping::Player;
use std::{collections::HashMap, sync::Arc, io};
use tokio::sync::{RwLock, Semaphore};
use tokio::process::Command;
use tokio::io::{AsyncBufReadExt, BufReader};
use colored::Colorize;
use chrono::prelude::*;
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
    #[sqlx(default)]
    no_answer_count: Option<i32>,
    #[sqlx(default)]
    last_answer: Option<DateTime<Utc>>,
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
        .connect("postgres://postgres:47tc34tcbterkfb349crewd@unesty.mcinfos.de/postgres").await.unwrap();
    println!("{}", "DB verbunden".green());
    let (tx, mut rx) = tokio::sync::mpsc::channel::<IpInfo>(10000);

    let scans: Arc<RwLock<HashMap<IpNetwork, Option<IpInfo>>>> = db_read_ips(&pool).await;
    let pool_cloned = pool.clone();
    let scans_anders = Arc::clone(&scans);
    task::spawn(async move {
        let mut bereits_gescanned: Vec<Update> = Vec::with_capacity(10000);
        let mut neue: Vec<IpInfo> = Vec::new();
        while let Some(e) = rx.recv().await {
                let mut guard = scans_anders.write().await;
                if let Some(Some(i)) = guard.get(&e.ip).clone() {
                    if i.version == e.version
                        &&i.protocol == e.protocol
                        &&i.max_players == e.max_players
                        &&i.description == e.description {
                            if i.online_players != e.online_players
                                || i.sample != e.sample {
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
        let insert = IpInfo {
            ip: e.ip,
            port: e.port,
            version: e.version,
            protocol: e.protocol,
            max_players: e.max_players,
            online_players: e.online_players,
            description: e.description,
            sample: e.sample,
            last_answer: Some(Utc::now()),
            no_answer_count: Some(0),
        };
        guard.insert(insert.ip, Some(insert.clone()));
        neue.push(insert);
        drop(guard);
            if neue.len() >= 100 || bereits_gescanned.len() >= 100 {
                let ergebnis = std::mem::take(&mut neue);
                let update = std::mem::take(&mut bereits_gescanned);
                let result = db_write(&pool_cloned, &ergebnis, update).await;
                match result {
                    true => (),
                    false => {println!("{}", "Fehler db_write".red());
                std::process::abort();},
                };
                continue;
            }
        }
    });
    let mut scans_pro_sek = String::new();
    let (keine_antwort_tx, mut keine_antwort_rx) = tokio::sync::mpsc::channel::<IpNetwork>(10000);
    println!("Wie viel gleichzeitige Tcp Verbindungen?");
    io::stdin()
        .read_line(&mut scans_pro_sek)
        .expect("Machs richtig");
    let mut typ = String::new();
    println!("Rescan, Ipv4 Scan, beides oder Ununsed Server cleanup 1/2/3/4");
    io::stdin()
        .read_line(&mut typ)
        .expect("Machs richtig");
    if typ.trim() == "1" {
        println!("{}", "Rescan gestartet".green());
        let scans_andere = Arc::clone(&scans);
        task::spawn(async move {
            while let Some(e) = keine_antwort_rx.recv().await {
            let mut guard = scans_andere.write().await;
            let ipinfo = guard.get(&e).unwrap();
            if let Some(ipinfos) = ipinfo {
            let no_answer_count = match ipinfos.no_answer_count {
                Some(e) => e + 1,
                None => 1,
            };
            let update = IpInfo {
                ip: ipinfos.ip,
                port: ipinfos.port,
                version: ipinfos.version.clone(),
                protocol: ipinfos.protocol,
                max_players: ipinfos.max_players,
                online_players: ipinfos.online_players,
                description: ipinfos.description.clone(),
                sample: ipinfos.sample.clone(),
                no_answer_count: Some(no_answer_count) ,
                last_answer: ipinfos.last_answer,
            };
            guard.insert(update.ip, Some(update));
            }}
        });
        loop {
            let scan = Arc::clone(&scans);
            let scans = scan.read().await;
            let tx = tx.clone();
            println!("Rescan start");
            rescan(tx, scans_pro_sek.clone().trim().parse::<i32>().unwrap(), scans.clone(), keine_antwort_tx.clone()).await;
            println!("{}", "Rescan fertig, weiterer Scan".green());
        }
    };
        if typ.trim() == "3" {
        let tx = tx.clone();
        let scans_pro_sek = scans_pro_sek.clone();
        let scans = scans.clone();
        task::spawn(async move{
            loop {
                let scan = Arc::clone(&scans);
                let scans = scan.read().await;
                let tx = tx.clone();
                println!("Rescan start");
                rescan(tx, scans_pro_sek.clone().trim().parse::<i32>().unwrap(), scans.clone(), keine_antwort_tx.clone()).await;
                println!("{}", "Rescan fertig, weiterer Scan".green());
            }
        });
    };
    let (ip_tx, mut ip_rx) = tokio::sync::mpsc::channel::<IpNetwork>(10000);
    task::spawn(async move {
        let mut zmap = Command::new("zmap").arg("-G").arg("82:01:a5:d2:71:56").arg("-p").arg("25565").arg("-b").arg("/etc/zmap/blacklist.conf").stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::inherit()).spawn().expect("Zmap");
        println!("{}", "Zmap gestartet".green());
        if let Some(stdout) = zmap.stdout.take() {
            let mut lines = BufReader::new(stdout).lines();
            while let Ok(Some(werte)) = lines.next_line().await {
                let getrimmed = werte.trim();
                if getrimmed.is_empty() {
                    continue;
                }
                match getrimmed.parse::<std::net::IpAddr>() {
                    Ok(ip) => {
                        let k = IpNetwork::from(ip);
                        let e = ip_tx.send(k).await;
                        match e {
                            Ok(_) => (),
                            Err(ee)=> println!("Fehler beim senden Zmap: {:?}", ee),

                        };
                    }
                    Err(e) => {println!("Fehler beim trimmen bei Zmap: {:?}", e);}
                }
            }
};
println!("If let hat kein Bock");
    });
    let sem = Arc::new(Semaphore::new(scans_pro_sek.trim().parse().unwrap()));
    loop {
    let mut set = JoinSet::new();
    while let Some(ip) = ip_rx.recv().await {
    let sem = Arc::clone(&sem);
    let scan = Arc::clone(&scans);
    let tx = tx.clone();
    while set.len() >= 1000 {
        set.join_next().await;
    }
    set.spawn(async move {
        let _permit = sem.clone().acquire_owned().await.unwrap();
        let guard = scan.read().await;
        if let Some(None) = guard.get(&ip) {
            println!("{}", "Opt-out".blue());
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
        let e = tx.send(sendung).await;
        match e {
            Ok(_) => (),
            Err(ee) => println!("{} {:?}", "Fehler senden im Channel:".red(), ee),

        }
    });
    }
    while let Some(s) = set.join_next().await {
        if let Err(e) = s {
            println!("{} {:?}", "Fehler:".red(), e)
        }
    }
}
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
                        no_answer_count: Some(0),
                        last_answer: Some(Utc::now()),
                };
            return Some(ergebnis);
        },
        _ => {return None},
    }
}
return None;
}
async fn db_write(pool: &Pool<Postgres>, ergebnisse1: &Vec<IpInfo>, bereits_gescanned: Vec<Update>) -> bool {
    if !ergebnisse1.is_empty() {
    let mut builder1 = QueryBuilder::<Postgres>::new("INSERT INTO servers (ip, port, version, protocol, max_players, description)");
    builder1.push_values(ergebnisse1, |mut b, inserts| {
        b.push_bind(inserts.ip)
         .push_bind(inserts.port)
         .push_bind(inserts.version.clone())
         .push_bind(inserts.protocol)
         .push_bind(inserts.max_players)
         .push_bind(inserts.description.clone());
    });
    let query1 = builder1.build();
    let result = query1.execute(pool).await;
    match result {
        Ok(_) => (),
        Err(e) => {println!("Fehler dbwrite Result: {:?}", e);
    return false},
    };}
    if !bereits_gescanned.is_empty() {
    let mut builder = QueryBuilder::<Postgres>::new("INSERT INTO status (ip, port, online_players, sample)");
    builder.push_values(bereits_gescanned, |mut b, updates| {
        b.push_bind(updates.ip)
         .push_bind(updates.port)
         .push_bind(updates.online_players)
         .push_bind(updates.sample);
    });
    let query = builder.build();
    let result2 = query.execute(pool).await;
    match result2 {
        Ok(_) => (),
        Err(e) => {println!("Fehler dbwrite Result2: {:?}", e);
    return false},
    };}
    println!("{}", "Ergebnisse erfolgreich geschrieben.".green());
    true
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
    println!("Länge Opt-Out: {}", ips.len());
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
    println!("Anzahl der Ips ohne Opt-Out: {}", ips.len());
    for ip in ips {
        let mut guard = scans.write().await;
        let ergebnis = IpInfo {
            ip: ip.ip,
            port: ip.port,
            version: ip.version,
            protocol: ip.protocol,
            max_players: ip.max_players,
            online_players: None,
            sample: None,
            description: ip.description,
            last_answer: None,
            no_answer_count: Some(0),
        };
        guard.insert(ip.ip, Some(ergebnis));
    }
    scans
}
async fn rescan(tx: tokio::sync::mpsc::Sender<IpInfo>, scans_pro_sek: i32, guard: HashMap<IpNetwork, Option<IpInfo>>, tx_no_answer: tokio::sync::mpsc::Sender<IpNetwork>
) {
    let sem = Arc::new(Semaphore::new(scans_pro_sek as usize));
    let mut set = JoinSet::new();
    let mut ips: Vec<IpNetwork> = Vec::new();
    for key in guard.keys() {
        ips.push(key.clone());
    };
    let guard = Arc::new(guard);
    for e in ips {
        let tx2 = tx_no_answer.clone();
        let guard = Arc::clone(&guard);
        let tx_clone = tx.clone();
        let sem = Arc::clone(&sem);
        set.spawn(async move {
            let ip: IpNetwork = e.clone();
            let _permit = sem.clone().acquire_owned().await.unwrap();
            let ergebnis = timeout(Duration::from_secs(4), pinger(ip)).await;
            if let Ok(ergebnis) = ergebnis {
            match ergebnis {
                Some(ping) => {
                    if guard.contains_key(&ip) {
                        let alte_infos = guard.get(&ip);
                        match alte_infos {
                            Some(Some(alte_info)) => {
                                if alte_info == &ping {
                                return
                                };
                                if &alte_info.ip != &ip {
                                println!("{}", "Hier ist etwas extrem schief gelaufen".red());
                                std::process::abort()
                                }
                                let insert = IpInfo {
                                    ip: ip,
                                    port: 25565 as i16,
                                    version: ping.version,
                                    protocol: ping.protocol as i16,
                                    max_players: ping.max_players as i32,
                                    online_players: ping.online_players,
                                    description: ping.description,
                                    sample: ping.sample,
                                    last_answer: Some(Utc::now()),
                                    no_answer_count: Some(0),
                                };
                                drop(guard);
                                let e = tx_clone.send(insert).await;
                                match e {
                                    Ok(_) => (),
                                    Err(ee) => println!("{} {:?}", "Fehler senden im Channel:".red(), ee),
                                }
                                return
                            }
                            _ => {println!("{}", "Uiii hier ist was falsch gegangen. Grund: Rescanner hat Ips bekommen die nicht gescanned werden sollten".red());}
                        }
                    }
                },
                None => {let result = tx2.send(ip).await;
                    match result {
                        Ok(_) => (),
                        Err(e) => {println!("{} {}", "Fehler nichtantwortende ip in den channel senden:".red(), e);
                        std::process::abort();
                    }
                    }
                    return;},
            };}
            return;
        });
        }
        while let Some(s) = set.join_next().await {
            if let Err(e) = s {
                println!("{} {:?}", "Fehler:".red(), e)
            }
        }
    }

