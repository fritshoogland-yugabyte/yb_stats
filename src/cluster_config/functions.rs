//! The impls and functions
//!
use chrono::Local;
use std::{sync::mpsc::channel, time::Instant};
use log::*;
use anyhow::{Result, Context};
use crate::isleader::AllIsLeader;
use crate::utility;
use crate::snapshot;
use crate::cluster_config::{AllSysClusterConfigEntryPB, SysClusterConfigEntryPB};
use crate::Opts;

impl SysClusterConfigEntryPB {
    fn new() -> Self { Default::default() }
}

impl AllSysClusterConfigEntryPB {
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allsysclusterconfigentrypb = AllSysClusterConfigEntryPB::read_cluster_config(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "cluster-config", allsysclusterconfigentrypb.sysclusterconfigentrypb)?;

        info!("end snapshot: {:?}", timer.elapsed());

        Ok(())
    }
    pub fn new() -> Self {
        Default::default()
    }
    pub async fn read_cluster_config(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllSysClusterConfigEntryPB
    {
        info!("begin parallel http read");
        let timer = Instant::now();

        let pool = rayon::ThreadPoolBuilder::new().num_threads(parallel).build().unwrap();
        let (tx, rx) = channel();
        pool.scope(move |s| {
            for host in hosts {
                for port in ports {
                    let tx = tx.clone();
                    s.spawn(move |_| {
                        let detail_snapshot_time = Local::now();
                        let mut cluster_config = AllSysClusterConfigEntryPB::read_http(host, port);
                        cluster_config.timestamp = Some(detail_snapshot_time);
                        cluster_config.hostname_port = Some(format!("{}:{}", host, port));
                        tx.send(cluster_config).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allsysclusterconfigentrypb = AllSysClusterConfigEntryPB::new();

        // the filter on the mpsc rx channel filters emptiness of the cluster_uuid field,
        // indicating the source was not a master leader or follower.
        for sysclusterconfigentrypb in rx.iter().filter(|r| !r.cluster_uuid.is_empty()) {
            allsysclusterconfigentrypb.sysclusterconfigentrypb.push(sysclusterconfigentrypb);
        }

        allsysclusterconfigentrypb
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> SysClusterConfigEntryPB
    {
        let data_from_http = utility::http_get(host, port, "api/v1/cluster-config");
        AllSysClusterConfigEntryPB::parse_cluster_config(data_from_http, host, port)
    }
    fn parse_cluster_config(
        http_data: String,
        host: &str,
        port: &str,
    ) -> SysClusterConfigEntryPB {
        serde_json::from_str(&http_data)
            .unwrap_or_else(|e| {
                debug!("({}:{}) could not parse /api/v1/cluster-config json data for cluster-info, error: {}", host, port, e);
                SysClusterConfigEntryPB::new()
            })
    }
    pub fn print(
        &self,
        leader_hostname: String
    ) -> Result<()>
    {

        println!("{}", serde_json::to_string_pretty( &self.sysclusterconfigentrypb
            .iter()
            .find(|r| r.hostname_port == Some(leader_hostname.clone()))
            .with_context(|| "Unable to find current master leader")?
            //.filter(|r| r.hostname_port == Some(leader_hostname.clone()))
            //.next()
        )?);
        Ok(())
    }
}

pub async fn print_cluster_config(
    hosts: Vec<&str>,
    ports: Vec<&str>,
    parallel: usize,
    options: &Opts,
) -> Result<()>
{
    match options.print_cluster_config.as_ref().unwrap() {
        Some(snapshot_number) => {

            let mut allsysclusterconfigentrypb = AllSysClusterConfigEntryPB::new();
            allsysclusterconfigentrypb.sysclusterconfigentrypb = snapshot::read_snapshot_json(snapshot_number, "cluster-config")?;
            let leader_hostname = AllIsLeader::return_leader_snapshot(snapshot_number)?;

            allsysclusterconfigentrypb.print(leader_hostname)?;

        }
        None => {
            let allsysclusterconfigentrypb = AllSysClusterConfigEntryPB::read_cluster_config(&hosts, &ports, parallel).await;
            let leader_hostname = AllIsLeader::return_leader_http(&hosts, &ports, parallel).await;
            allsysclusterconfigentrypb.print(leader_hostname)?;
        }
    }
    Ok(())
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unit_parse_simple_data() {
        let json = r#"
{
    "version":0,
    "cluster_uuid":"6cfdbce0-b98d-4aed-a5ec-372a726258b2"
}
        "#.to_string();
        let result = AllSysClusterConfigEntryPB::parse_cluster_config(json, "", "");
        //println!("{:#?}", result);
        assert_eq!(result.version, 0);
        assert_eq!(result.cluster_uuid, "6cfdbce0-b98d-4aed-a5ec-372a726258b2");
    }

    #[test]
    fn unit_parse_xcluster_original_data() {
        let json = r#"
        {"version":54,"replication_info":{"live_replicas":{"num_replicas":3,"placement_blocks":[{"cloud_info":{"placement_cloud":"aws","placement_region":"us-west-2","placement_zone":"us-west-2a"},"min_num_replicas":3}],"placement_uuid":"29304744-f481-41a0-8d65-b613f453341e"},"affinitized_leaders":[{"placement_cloud":"aws","placement_region":"us-west-2","placement_zone":"us-west-2a"}]},"cluster_uuid":"63edb5bd-8855-41b8-bb64-67d611235f1e","consumer_registry":{"producer_map":[{"key":"db8329ec-b249-490c-96d0-cbe6bfa6f0b6_setup1","value":{"stream_map":[{"key":"3a395c2133004d90bb0e572c727174bd","value":{"consumer_producer_tablet_map":[{"key":"99f5bfb8728e4502a1c178c686063ab9","value":{"tablets":["c0e5c49f8a604b82b19cf93ea841d267"],"start_key":["Ã¾"],"end_key":["Â•S"]}},{"key":"b4ccea2912cf48b68e54982e39e89f0c","value":{"tablets":["769ff9cdabc0413b9d0b8c7161163f00"],"start_key":["UT"],"end_key":["jÂ©"]}},{"key":"11793b7c83a24774b429fd7e3aaaf646","value":{"tablets":["a5b41af81c9f42b08b349fb2493918cc"],"start_key":["Â•S"],"end_key":["ÂªÂ¨"]}},{"key":"d4ab089aeeb347ceb414ce6b6e80ddd3","value":{"tablets":["3d4f0fe698eb490287f0ba2fb8143503"],"start_key":["ÃªÂ§"],"end_key":[""]}},{"key":"efac320bde7c42ee8e6590db469f70ff","value":{"tablets":["46a7b0ca41b64368aea597592cfd452a"],"start_key":["?Ã¿"],"end_key":["UT"]}},{"key":"3b04ba8c21034a898366cdb44ceabf24","value":{"tablets":["635c958e767049cf9e4b05bf116ebc27"],"start_key":["ÂªÂ¨"],"end_key":["Â¿Ã½"]}},{"key":"c685cbe6383a4ec98576a59fe0496ff2","value":{"tablets":["b4278cadf2a04a85b95ce9b18943115f"],"start_key":["Ã•R"],"end_key":["ÃªÂ§"]}},{"key":"41a18d26548045859bc1fe5330d6ac99","value":{"tablets":["7f21f799653b47eca77b9fe7a56b9512"],"start_key":["*Âª"],"end_key":["?Ã¿"]}},{"key":"f11633f2e7cd4effa7c0fa762a1b3bb3","value":{"tablets":["032e1ad9e4014d31a3aec34347d9d62d"],"start_key":["\u0015U"],"end_key":["*Âª"]}},{"key":"8b2b69cbb7b24400a677baaaa07125be","value":{"tablets":["2889354fc3f64c518f63abdf8ab26e1a"],"start_key":[""],"end_key":["\u0015U"]}},{"key":"adeec6e9ba1149bb93dc1fdfa2bac5f9","value":{"tablets":["72865a4a541640f9b008373154d26eba"],"start_key":["jÂ©"],"end_key":["Ã¾"]}},{"key":"acafa8602fbc4690b7a62aaa1c2fc81e","value":{"tablets":["8805f821629b4a259aff3569804e5016"],"start_key":["Â¿Ã½"],"end_key":["Ã•R"]}}],"consumer_table_id":"000033e6000030008000000000004008","producer_table_id":"000033e600003000800000000000400a","local_tserver_optimized":true}},{"key":"71f6cb57f597453f86f2f9e3c0d7c1f4","value":{"consumer_producer_tablet_map":[{"key":"c7bb5180672a4d5bb5cb27bc3ff7a482","value":{"tablets":["3339e43f308746cfbc0e25616ef94dab"],"start_key":["uN"],"end_key":["Ã¸"]}},{"key":"0ef3aa3a7e9f45fa8c5097796c36d992","value":{"tablets":["d127d2ad642a4747a33b37babb258c8e"],"start_key":["JÂ¦"],"end_key":["UP"]}},{"key":"ce60a509899141a2afc82cc9f195d719","value":{"tablets":["7ab8bf8222ce44d98b9d1639644eb09e"],"start_key":["ÃŠÂž"],"end_key":["Ã•H"]}},{"key":"b69c88429f1941119e755983ff26c917","value":{"tablets":["74269d7c9d5a4273a248848527c647c1"],"start_key":["Ã¸"],"end_key":["ÂŠÂ¢"]}},{"key":"0d6cdd8e67a24e20bd45384025a3f71a","value":{"tablets":["daaaacb684f54a30adc19653174fa13c"],"start_key":["ÂŸÃ¶"],"end_key":["ÂªÂ "]}},{"key":"c8dd9e0659bb484ebad54aaa9c1db621","value":{"tablets":["c2e28aa89ec743c0be22efc9c01cdf5a"],"start_key":["ÃªÂœ"],"end_key":["ÃµF"]}},{"key":"99e9f5717e5c471a8d9eaa63410f2efe","value":{"tablets":["7cf8598eafdc42fdb44e338101186f11"],"start_key":["ÃŸÃ²"],"end_key":["ÃªÂœ"]}},{"key":"b3010b9f3399431dbc59218fdbe1f77c","value":{"tablets":["8ce1272cc527442c9ea5e51a19136123"],"start_key":["jÂ¤"],"end_key":["uN"]}},{"key":"0a91784b6e8f4393936aced2a9824ebc","value":{"tablets":["8ce5789cbad642059a997dff21d54e37"],"start_key":["\u001FÃ¾"],"end_key":["*Â¨"]}},{"key":"71da5bc56df74fc2a9d4ff61f52c4f54","value":{"tablets":["d77914846b1e44d8a5abd7aac1b38efb"],"start_key":["UP"],"end_key":["_Ãº"]}},{"key":"37b810ba5cd44f2ab48c32275ffe1bd2","value":{"tablets":["49e12e1bc6084e08a02ddfda70e8d04a"],"start_key":["5R"],"end_key":["?Ã¼"]}},{"key":"9cff88e5e8b44b6497968e55ea582b61","value":{"tablets":["5bc81b0316ee47b2be8e60a31d08ec55"],"start_key":["\nÂª"],"end_key":["\u0015T"]}},{"key":"cfddf0d0bd2541ae882978a18088f5a4","value":{"tablets":["9cc28681989e412cbb51272619a34c46"],"start_key":["*Â¨"],"end_key":["5R"]}},{"key":"8a2a30c94e3747a3beb8118e3baa93f0","value":{"tablets":["457024d2e596468a8131932a8d17cca5"],"start_key":["ÃµF"],"end_key":[""]}},{"key":"91f3e7c0dde84809b05b16f08d8e1c16","value":{"tablets":["d264aace2dbb4b9c83b1e5d93529129e"],"start_key":[""],"end_key":["\nÂª"]}},{"key":"4f3ad331b22f417dbb3982314f4a2087","value":{"tablets":["4e549ce7754246b785c4c94b12aa314a"],"start_key":["Â•L"],"end_key":["ÂŸÃ¶"]}},{"key":"e4d53b4a2c1f46119329feb4b55de807","value":{"tablets":["c1413393c8db4f8da7d0660a3fdd016b"],"start_key":["Â¿Ã´"],"end_key":["ÃŠÂž"]}},{"key":"858e4d9101e84d409ef65181b0ca019e","value":{"tablets":["05c94dec265242c780d8e276fab965a8"],"start_key":["?Ã¼"],"end_key":["JÂ¦"]}},{"key":"d957124d6fda4e7e871b3e9f81a315fa","value":{"tablets":["46d4c47b5176486f868322c4c57efd5e"],"start_key":["ÂªÂ "],"end_key":["ÂµJ"]}},{"key":"d3eddc99d3e647a683308ed9d2ffa79a","value":{"tablets":["38b9035a4e014e7cb970fbe2ababbdc8"],"start_key":["ÂŠÂ¢"],"end_key":["Â•L"]}},{"key":"34259ade4c134381b9fa16fe01216b58","value":{"tablets":["66f515d7fe3f454f9511b0e48a62e8cf"],"start_key":["\u0015T"],"end_key":["\u001FÃ¾"]}},{"key":"89f37e132f494b4bbbba962f5462580b","value":{"tablets":["6c61e117f55849788a847f835edf8dc6"],"start_key":["Ã•H"],"end_key":["ÃŸÃ²"]}},{"key":"924dacdf92cd4e6dabc768d27ddd7abb","value":{"tablets":["f567973e900e4aee8c5a1e5e5e4b1cc9"],"start_key":["ÂµJ"],"end_key":["Â¿Ã´"]}},{"key":"04947f359c64448797e7ab73ac0bd6a2","value":{"tablets":["12e132ce6522488697a3442c947296fa"],"start_key":["_Ãº"],"end_key":["jÂ¤"]}}],"consumer_table_id":"bdd49b6fd52f4dd987a414d049d06c8f","producer_table_id":"9c368134314d4f71a9947a262d809d11","local_tserver_optimized":true}},{"key":"b00f417f5d774f16ba89ab24499a8bb3","value":{"consumer_producer_tablet_map":[{"key":"ef4afa0510e940c9bc3fb47b90f641f4","value":{"tablets":["af41aea796e34f87b150c6bdd53585e9"],"start_key":["ÂªÂ¨"],"end_key":["Â¿Ã½"]}},{"key":"bf631be03ed84b118bbf0faaf27856a9","value":{"tablets":["c44b50345f6f4eb1be75763761cbb439"],"start_key":["\u0015U"],"end_key":["*Âª"]}},{"key":"f5bb9fb82ab94007bc49dd9010e1bfa7","value":{"tablets":["bb2010ef6ecf4b198017b8d8ede1e5ae"],"start_key":["Â•S"],"end_key":["ÂªÂ¨"]}},{"key":"4cb12be1f6c54204a3e68007156174c3","value":{"tablets":["940d7eb7fc254d02b7e8997143a209ee"],"start_key":["Ã•R"],"end_key":["ÃªÂ§"]}},{"key":"3e43c1543a8b4075a82ba7918de26a9b","value":{"tablets":["2f8a25e881f146c9be996e0d858fa833"],"start_key":["jÂ©"],"end_key":["Ã¾"]}},{"key":"a67141f9b73d42fa9450d9397fdaada2","value":{"tablets":["f1d4632a95b14fdda0aaaae5267055b8"],"start_key":["*Âª"],"end_key":["?Ã¿"]}},{"key":"641477a99221440f9ffe9bd793ccd345","value":{"tablets":["ddde606635934ffb9ab56df975cc3788"],"start_key":["?Ã¿"],"end_key":["UT"]}},{"key":"c06ca4db5f3f4c95bacf8dd42c3a8fc3","value":{"tablets":["6c9ba6b4acdb47a58052f4477c557880"],"start_key":["Ã¾"],"end_key":["Â•S"]}},{"key":"8f867a862d9b4010890a7ef03c66dc42","value":{"tablets":["28e6154cfbe94e1a8bafc8210573da38"],"start_key":["Â¿Ã½"],"end_key":["Ã•R"]}},{"key":"6a15ec4c4a3b44b09bbc8f18623ff050","value":{"tablets":["c644fd2e389249828517029524da9d0a"],"start_key":["ÃªÂ§"],"end_key":[""]}},{"key":"39382e133dfa42b4a470ef0831d3761c","value":{"tablets":["f71eb98a74ae4a8dae073a4d3f6ba510"],"start_key":["UT"],"end_key":["jÂ©"]}},{"key":"b6758399a4b54dda9259231d669d15d8","value":{"tablets":["ff110094bd4d493caeafdd5489f048d3"],"start_key":[""],"end_key":["\u0015U"]}}],"consumer_table_id":"000033e6000030008000000000004005","producer_table_id":"000033e6000030008000000000004005","local_tserver_optimized":true,"producer_schema":{"validated_schema_version":2}}}],"master_addrs":[{"host":"172.151.17.239","port":7100},{"host":"172.151.24.171","port":7100},{"host":"172.151.22.212","port":7100}]}}]},"leader_blacklist":{"initial_leader_load":0}}
        "#.to_string();
        let result = AllSysClusterConfigEntryPB::parse_cluster_config(json, "", "");
        println!("{:#?}", result);
        //assert!(result.version, 0);
        //assert!(result.cluster_uuid, "6cfdbce0-b98d-4aed-a5ec-372a726258b2");
    }

    #[test]
    fn unit_parse_consumer_registry_data() {
        let json = r#"
{
    "version":54,
    "cluster_uuid":"63edb5bd-8855-41b8-bb64-67d611235f1e",
    "consumer_registry":
    {
        "producer_map":
        [
            {
                "key":"db8329ec-b249-490c-96d0-cbe6bfa6f0b6_setup1",
                "value":
                {
                    "stream_map":
                    [
                        {
                            "key":"3a395c2133004d90bb0e572c727174bd",
                            "value":
                            {
                                "consumer_producer_tablet_map":
                                [
                                    {"key":"99f5bfb8728e4502a1c178c686063ab9", "value": { "tablets": ["c0e5c49f8a604b82b19cf93ea841d267"], "start_key":["Ã¾"], "end_key":["Â•S"] } },
                                    {"key":"3b04ba8c21034a898366cdb44ceabf24","value":{"tablets":["635c958e767049cf9e4b05bf116ebc27"],"start_key":["ÂªÂ¨"],"end_key":["Â¿Ã½"]}},
                                    {"key":"c685cbe6383a4ec98576a59fe0496ff2","value":{"tablets":["b4278cadf2a04a85b95ce9b18943115f"],"start_key":["Ã•R"],"end_key":["ÃªÂ§"]}},
                                    {"key":"41a18d26548045859bc1fe5330d6ac99","value":{"tablets":["7f21f799653b47eca77b9fe7a56b9512"],"start_key":["*Âª"],"end_key":["?Ã¿"]}},
                                    {"key":"f11633f2e7cd4effa7c0fa762a1b3bb3","value":{"tablets":["032e1ad9e4014d31a3aec34347d9d62d"],"start_key":["\u0015U"],"end_key":["*Âª"]}},
                                    {"key":"8b2b69cbb7b24400a677baaaa07125be","value":{"tablets":["2889354fc3f64c518f63abdf8ab26e1a"],"start_key":[""],"end_key":["\u0015U"]}},
                                    {"key":"adeec6e9ba1149bb93dc1fdfa2bac5f9","value":{"tablets":["72865a4a541640f9b008373154d26eba"],"start_key":["jÂ©"],"end_key":["Ã¾"]}},
                                    {"key":"acafa8602fbc4690b7a62aaa1c2fc81e","value":{"tablets":["8805f821629b4a259aff3569804e5016"],"start_key":["Â¿Ã½"],"end_key":["Ã•R"]}}
                                ],
                                "consumer_table_id":"000033e6000030008000000000004008",
                                "producer_table_id":"000033e600003000800000000000400a",
                                "local_tserver_optimized":true
                            }
                        },
                        {
                            "key":"71f6cb57f597453f86f2f9e3c0d7c1f4",
                            "value":
                            {
                                "consumer_producer_tablet_map":
                                [
                                    {"key":"c7bb5180672a4d5bb5cb27bc3ff7a482","value":{"tablets":["3339e43f308746cfbc0e25616ef94dab"],"start_key":["uN"],"end_key":["Ã¸"]}},
                                    {"key":"0ef3aa3a7e9f45fa8c5097796c36d992","value":{"tablets":["d127d2ad642a4747a33b37babb258c8e"],"start_key":["JÂ¦"],"end_key":["UP"]}},
                                    {"key":"ce60a509899141a2afc82cc9f195d719","value":{"tablets":["7ab8bf8222ce44d98b9d1639644eb09e"],"start_key":["ÃŠÂž"],"end_key":["Ã•H"]}},
                                    {"key":"b69c88429f1941119e755983ff26c917","value":{"tablets":["74269d7c9d5a4273a248848527c647c1"],"start_key":["Ã¸"],"end_key":["ÂŠÂ¢"]}},
                                    {"key":"0d6cdd8e67a24e20bd45384025a3f71a","value":{"tablets":["daaaacb684f54a30adc19653174fa13c"],"start_key":["ÂŸÃ¶"],"end_key":["ÂªÂ "]}},
                                    {"key":"c8dd9e0659bb484ebad54aaa9c1db621","value":{"tablets":["c2e28aa89ec743c0be22efc9c01cdf5a"],"start_key":["ÃªÂœ"],"end_key":["ÃµF"]}},
                                    {"key":"99e9f5717e5c471a8d9eaa63410f2efe","value":{"tablets":["7cf8598eafdc42fdb44e338101186f11"],"start_key":["ÃŸÃ²"],"end_key":["ÃªÂœ"]}},
                                    {"key":"b3010b9f3399431dbc59218fdbe1f77c","value":{"tablets":["8ce1272cc527442c9ea5e51a19136123"],"start_key":["jÂ¤"],"end_key":["uN"]}},
                                    {"key":"0a91784b6e8f4393936aced2a9824ebc","value":{"tablets":["8ce5789cbad642059a997dff21d54e37"],"start_key":["\u001FÃ¾"],"end_key":["*Â¨"]}},
                                    {"key":"71da5bc56df74fc2a9d4ff61f52c4f54","value":{"tablets":["d77914846b1e44d8a5abd7aac1b38efb"],"start_key":["UP"],"end_key":["_Ãº"]}},
                                    {"key":"37b810ba5cd44f2ab48c32275ffe1bd2","value":{"tablets":["49e12e1bc6084e08a02ddfda70e8d04a"],"start_key":["5R"],"end_key":["?Ã¼"]}},
                                    {"key":"9cff88e5e8b44b6497968e55ea582b61","value":{"tablets":["5bc81b0316ee47b2be8e60a31d08ec55"],"start_key":["\nÂª"],"end_key":["\u0015T"]}},
                                    {"key":"cfddf0d0bd2541ae882978a18088f5a4","value":{"tablets":["9cc28681989e412cbb51272619a34c46"],"start_key":["*Â¨"],"end_key":["5R"]}},
                                    {"key":"8a2a30c94e3747a3beb8118e3baa93f0","value":{"tablets":["457024d2e596468a8131932a8d17cca5"],"start_key":["ÃµF"],"end_key":[""]}},
                                    {"key":"91f3e7c0dde84809b05b16f08d8e1c16","value":{"tablets":["d264aace2dbb4b9c83b1e5d93529129e"],"start_key":[""],"end_key":["\nÂª"]}},
                                    {"key":"4f3ad331b22f417dbb3982314f4a2087","value":{"tablets":["4e549ce7754246b785c4c94b12aa314a"],"start_key":["Â•L"],"end_key":["ÂŸÃ¶"]}},
                                    {"key":"e4d53b4a2c1f46119329feb4b55de807","value":{"tablets":["c1413393c8db4f8da7d0660a3fdd016b"],"start_key":["Â¿Ã´"],"end_key":["ÃŠÂž"]}},
                                    {"key":"858e4d9101e84d409ef65181b0ca019e","value":{"tablets":["05c94dec265242c780d8e276fab965a8"],"start_key":["?Ã¼"],"end_key":["JÂ¦"]}},
                                    {"key":"d957124d6fda4e7e871b3e9f81a315fa","value":{"tablets":["46d4c47b5176486f868322c4c57efd5e"],"start_key":["ÂªÂ "],"end_key":["ÂµJ"]}},
                                    {"key":"d3eddc99d3e647a683308ed9d2ffa79a","value":{"tablets":["38b9035a4e014e7cb970fbe2ababbdc8"],"start_key":["ÂŠÂ¢"],"end_key":["Â•L"]}},
                                    {"key":"34259ade4c134381b9fa16fe01216b58","value":{"tablets":["66f515d7fe3f454f9511b0e48a62e8cf"],"start_key":["\u0015T"],"end_key":["\u001FÃ¾"]}},
                                    {"key":"89f37e132f494b4bbbba962f5462580b","value":{"tablets":["6c61e117f55849788a847f835edf8dc6"],"start_key":["Ã•H"],"end_key":["ÃŸÃ²"]}},
                                    {"key":"924dacdf92cd4e6dabc768d27ddd7abb","value":{"tablets":["f567973e900e4aee8c5a1e5e5e4b1cc9"],"start_key":["ÂµJ"],"end_key":["Â¿Ã´"]}},
                                    {"key":"04947f359c64448797e7ab73ac0bd6a2","value":{"tablets":["12e132ce6522488697a3442c947296fa"],"start_key":["_Ãº"],"end_key":["jÂ¤"]}}
                                ],
                                "consumer_table_id":"bdd49b6fd52f4dd987a414d049d06c8f",
                                "producer_table_id":"9c368134314d4f71a9947a262d809d11",
                                "local_tserver_optimized":true
                            }
                        },
                        {
                            "key":"b00f417f5d774f16ba89ab24499a8bb3",
                            "value":
                            {
                                "consumer_producer_tablet_map":
                                [
                                    {"key":"ef4afa0510e940c9bc3fb47b90f641f4","value":{"tablets":["af41aea796e34f87b150c6bdd53585e9"],"start_key":["ÂªÂ¨"],"end_key":["Â¿Ã½"]}},
                                    {"key":"bf631be03ed84b118bbf0faaf27856a9","value":{"tablets":["c44b50345f6f4eb1be75763761cbb439"],"start_key":["\u0015U"],"end_key":["*Âª"]}},
                                    {"key":"f5bb9fb82ab94007bc49dd9010e1bfa7","value":{"tablets":["bb2010ef6ecf4b198017b8d8ede1e5ae"],"start_key":["Â•S"],"end_key":["ÂªÂ¨"]}},
                                    {"key":"4cb12be1f6c54204a3e68007156174c3","value":{"tablets":["940d7eb7fc254d02b7e8997143a209ee"],"start_key":["Ã•R"],"end_key":["ÃªÂ§"]}},
                                    {"key":"3e43c1543a8b4075a82ba7918de26a9b","value":{"tablets":["2f8a25e881f146c9be996e0d858fa833"],"start_key":["jÂ©"],"end_key":["Ã¾"]}},
                                    {"key":"a67141f9b73d42fa9450d9397fdaada2","value":{"tablets":["f1d4632a95b14fdda0aaaae5267055b8"],"start_key":["*Âª"],"end_key":["?Ã¿"]}},
                                    { "key":"641477a99221440f9ffe9bd793ccd345","value":{"tablets":["ddde606635934ffb9ab56df975cc3788"],"start_key":["?Ã¿"],"end_key":["UT"]}},
                                    { "key":"c06ca4db5f3f4c95bacf8dd42c3a8fc3", "value":{"tablets":["6c9ba6b4acdb47a58052f4477c557880"],"start_key":["Ã¾"],"end_key":["Â•S"]} },
                                    { "key":"8f867a862d9b4010890a7ef03c66dc42", "value":{"tablets":["28e6154cfbe94e1a8bafc8210573da38"],"start_key":["Â¿Ã½"],"end_key":["Ã•R"]} },
                                    { "key":"6a15ec4c4a3b44b09bbc8f18623ff050", "value":{"tablets":["c644fd2e389249828517029524da9d0a"],"start_key":["ÃªÂ§"],"end_key":[""]} },
                                    { "key":"39382e133dfa42b4a470ef0831d3761c", "value":{"tablets":["f71eb98a74ae4a8dae073a4d3f6ba510"],"start_key":["UT"],"end_key":["jÂ©"]} },
                                    { "key":"b6758399a4b54dda9259231d669d15d8", "value": { "tablets":["ff110094bd4d493caeafdd5489f048d3"], "start_key":[""], "end_key":["\u0015U"] } }
                                ],
                                "consumer_table_id":"000033e6000030008000000000004005",
                                "producer_table_id":"000033e6000030008000000000004005",
                                "local_tserver_optimized":true,
                                "producer_schema":{"validated_schema_version":2}
                            }
                        }
                    ],
                    "master_addrs":
                    [
                        {"host":"172.151.17.239","port":7100},
                        {"host":"172.151.24.171","port":7100},
                        {"host":"172.151.22.212","port":7100}
                    ]
                }
            }
        ]
    },
    "leader_blacklist":{"initial_leader_load":0}
}
        "#.to_string();
        let result = AllSysClusterConfigEntryPB::parse_cluster_config(json, "", "");
        println!("{:#?}", result);
        //assert!(result.version, 0);
        //assert!(result.cluster_uuid, "6cfdbce0-b98d-4aed-a5ec-372a726258b2");
    }

    #[test]
    fn unit_parse_consumer_limited_registry_data() {
        let json = r#"
{
    "version":54,
    "cluster_uuid":"63edb5bd-8855-41b8-bb64-67d611235f1e",
    "consumer_registry":
    {
        "producer_map":
        [
            {
                "key":"db8329ec-b249-490c-96d0-cbe6bfa6f0b6_setup1",
                "value":
                {
                    "stream_map":
                    [
                        {
                            "key":"3a395c2133004d90bb0e572c727174bd",
                            "value":
                            {
                                "consumer_producer_tablet_map":
                                [
                                ],
                                "consumer_table_id":"000033e6000030008000000000004005",
                                "producer_table_id":"000033e6000030008000000000004005",
                                "local_tserver_optimized":true,
                                "producer_schema":{"validated_schema_version":2}
                            }
                        }
                    ],
                    "master_addrs":
                    [
                        {"host":"172.151.17.239","port":7100},
                        {"host":"172.151.24.171","port":7100},
                        {"host":"172.151.22.212","port":7100}
                    ]
                }
            }
        ]
    },
    "leader_blacklist":{"initial_leader_load":0}
}
        "#.to_string();
        let result = AllSysClusterConfigEntryPB::parse_cluster_config(json, "", "");
        println!("{:#?}", result);
        //assert!(result.version, 0);
        //assert!(result.cluster_uuid, "6cfdbce0-b98d-4aed-a5ec-372a726258b2");
    }

    #[test]
    fn unit_parse_replication_info() {
        let json = r#"
{
    "version":54,
    "replication_info":
    {
        "live_replicas":
        {
            "num_replicas":3,
            "placement_blocks":
            [
                {
                    "cloud_info":
                    {
                        "placement_cloud":"aws",
                        "placement_region":"us-west-2",
                        "placement_zone":"us-west-2a"
                    },
                    "min_num_replicas":3
                }
            ],
            "placement_uuid":"29304744-f481-41a0-8d65-b613f453341e"
        },
        "affinitized_leaders":
        [
            {
                "placement_cloud":"aws",
                "placement_region":"us-west-2",
                "placement_zone":"us-west-2a"
            }
        ]
    },
    "cluster_uuid":"63edb5bd-8855-41b8-bb64-67d611235f1e"
}
        "#.to_string();
        let result = AllSysClusterConfigEntryPB::parse_cluster_config(json, "", "");
        println!("{:#?}", result);
        //assert!(result.version, 0);
        //assert!(result.cluster_uuid, "6cfdbce0-b98d-4aed-a5ec-372a726258b2");
    }

    #[test]
    fn unit_parse_cluster_config_simple_2() {
        let json = r#"
{
    "version":9,
    "replication_info":
    {
        "live_replicas":
        {
            "num_replicas":3,
            "placement_blocks":
            [
                {
                    "cloud_info":
                    {
                        "placement_cloud":"aws",
                        "placement_region":"us-east-2",
                        "placement_zone":"us-east-2a"
                    },
                    "min_num_replicas":3
                }
            ],
            "placement_uuid":"3dbd0636-780b-4b5a-9779-bdfe20b7ad87"
        },
        "affinitized_leaders":
        [
            {
                "placement_cloud":"aws",
                "placement_region":"us-east-2",
                "placement_zone":"us-east-2a"
            }
        ]
    },
    "cluster_uuid":"44989a28-d951-43aa-bbb5-f065c4e0bd1f",
    "leader_blacklist":
    {
        "initial_leader_load":0
    }
}
        "#.to_string();
        let result = AllSysClusterConfigEntryPB::parse_cluster_config(json, "", "");
        println!("{:#?}", result);
        //assert!(result.version, 0);
        //assert!(result.cluster_uuid, "6cfdbce0-b98d-4aed-a5ec-372a726258b2");
    }

    // this tests placement blocks, affinitized leaders, server_blacklist.
    #[test]
    fn unit_parse_cluster_config_simple_3() {
        let json = r#"
{   "version":232,
    "replication_info":
    {   "live_replicas":
        {   "num_replicas":3,
            "placement_blocks":
            [
                {   "cloud_info":
                    {   "placement_cloud":"gcp",
                        "placement_region":"us-west1",
                        "placement_zone":"us-west1-a"
                    },
                    "min_num_replicas":1
                },
                {   "cloud_info":
                    {   "placement_cloud":"gcp",
                        "placement_region":"us-west1",
                        "placement_zone":"us-west1-b"
                    },
                    "min_num_replicas":1
                },
                {   "cloud_info":
                    {   "placement_cloud":"gcp",
                        "placement_region":"us-west1",
                        "placement_zone":"us-west1-c"
                    },
                    "min_num_replicas":1
                }
            ],
            "placement_uuid":"4d9834cc-6d6e-4dc4-89ef-6a8590f59f43"
        },
        "affinitized_leaders":
        [
            {   "placement_cloud":"gcp",
                "placement_region":"us-west1",
                "placement_zone":"us-west1-a"
            },
            {   "placement_cloud":"gcp",
                "placement_region":"us-west1",
                "placement_zone":"us-west1-b"
            },
            {   "placement_cloud":"gcp",
                "placement_region":"us-west1",
                "placement_zone":"us-west1-c"
            }
        ]
    },
    "server_blacklist":
    {   "hosts":
        [
            {   "host":"10.150.0.116","port":9100   },
            {   "host":"10.150.0.50","port":9100    },
            {   "host":"10.150.255.150","port":9100 }
        ],
        "initial_replica_load":0
    },
    "cluster_uuid":"20a8f6fb-a385-40af-bb5c-5272368e13cf",
    "leader_blacklist":
    {   "initial_leader_load":0 }
}
        "#.to_string();
        let result = AllSysClusterConfigEntryPB::parse_cluster_config(json, "", "");
        println!("{:#?}", result);
        //assert!(result.version, 0);
        //assert!(result.cluster_uuid, "6cfdbce0-b98d-4aed-a5ec-372a726258b2");
    }

    #[tokio::test]
    async fn integration_parse_cluster_config() {
        let hostname = utility::get_hostname_master();
        let port = utility::get_port_master();

        let allsysclusterconfigentrypb = AllSysClusterConfigEntryPB::read_cluster_config(&vec![&hostname], &vec![&port], 1).await;

        assert!(!allsysclusterconfigentrypb.sysclusterconfigentrypb.is_empty());
    }

}
