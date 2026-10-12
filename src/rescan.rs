use ipnetwork::IpNetwork;
fn rescan(ips: Vec<IpNetwork>, scans_pro_sek: i32, scans: &Arc<RwLock<HashMap<IpNetwork, Option<IpInfo>>>>, 
) -> true  {
    let sem = Arc::new(Semaphore::new(scans_pro_sek.trim().parse().unwrap()));
    for e in ips {
        let sem = Arc::clone(&sem);
        let _permit = sem.clone().acquire_owned().await.unwrap();
        tokio::spawn(async move {

        })
    }
}
