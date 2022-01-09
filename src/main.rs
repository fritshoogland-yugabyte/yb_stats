mod latency_statistic_details;
mod value_statistic_details;
mod parse_json;

use structopt::StructOpt;
use port_scanner::scan_port_addr;
use std::process;
use std::collections::BTreeMap;
use std::io::stdin;
use std::time::SystemTime;
use regex::Regex;
use substring::Substring;

use yb_stats::{NamedMetrics, XValues, XLatencies, LatencyStatisticDetails, ValueStatisticDetails};

#[derive(Debug, StructOpt)]
struct Opts {
    #[structopt(short, long, default_value = "192.168.66.80:7000,192.168.66.81:7000,192.168.66.82:7000")]
    metric_sources: String,
    #[structopt(short, long, default_value = ".*")]
    stat_name_match: String,
    #[structopt(short, long, default_value = ".*")]
    table_name_match: String,
    #[structopt(short, long, default_value = "2")]
    wait_time: i32,
    #[structopt(short, long)]
    begin_end_mode: bool,
    #[structopt(short, long)]
    gauges_enable: bool,
}

fn main()
{
    let latency_statistic_details_lookup = latency_statistic_details::create_hashmap();
    let value_statistic_details_lookup = value_statistic_details::create_hashmap();

    // create variables based on StructOpt values
    let options = Opts::from_args();
    let metric_sources_vec: Vec<&str> = options.metric_sources.split(",").collect();
    let stat_name_match = &options.stat_name_match.as_str();
    let stat_name_filter = Regex::new(stat_name_match).unwrap();
    let table_name_match = &options.table_name_match.as_str();
    let table_name_filter = Regex::new(table_name_match).unwrap();
    let wait_time = options.wait_time as u64;
    let begin_end_mode = options.begin_end_mode as bool;
    let gauges_enable = options.gauges_enable as bool;

    // the bail_out boolean is used for 'begin-end mode' to quit the execution (bail out) the second time.
    let mut bail_out = false;
    // the first_pass boolean is used to determine the special case that we have no previous values.
    let mut first_pass = true;
    let mut fetch_time = SystemTime::now();
    let mut previous_fetch_time = SystemTime::now();

    // these are the definitions of the two types of statistics that are obtained and parsed from the specified metric sources.
    // These are nested btreemaps.
    // A btreemap will automatically order its contents.
    // The levels in the btreemap are: hostname:port, type (cluster, server, table, tablet), id, statistic name, statistic properties + previous values of total_count, total_sum or value and (measurement) time.
    let mut value_statistics: BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, XValues>>>> = BTreeMap::new();
    let mut latency_statistics: BTreeMap<String, BTreeMap<String, BTreeMap<String, BTreeMap<String, XLatencies>>>> = BTreeMap::new();

    loop {
        for hostname in &metric_sources_vec {
            if !scan_port_addr(hostname) {
                println!("Warning, unresponsive: {}", hostname.to_string());
                continue;
            };
            fetch_time = SystemTime::now();
            previous_fetch_time = if first_pass { fetch_time } else { previous_fetch_time };
            let metrics_data = reqwest::blocking::get(format!("http://{}/metrics", hostname.to_string()))
                .unwrap_or_else(|e| {
                    eprintln!("Error reading from URL: {}", e);
                    process::exit(1);
                })
                .text().unwrap();
            let metrics_parse = parse_json::parse_metrics(metrics_data);

            // a metric is a unit such as cluster, server, table or tablet.
            // it can contain a lot of actual statistics
            for metric in &metrics_parse {

                /*
                  These are the main properties that make a metric unique:
                  metrics_type (cluster, server, table, tablet)
                  metrics_id   (yb.cluster for cluster, yb.tabletserver for tabletserver, yb.master for master,
                                on the master '00000000000000000000000000000000' for the sys.catalog tablet, sys.catalog.uuid for the sys.catalog table,
                                and a UUID for table and tablet types)
                  metrics_attribute_namespace (the namespace for table and tablets, otherwise empty)
                  metrics_attribute_table_name (the name of the table for table and tablets, otherwise empty)
                 */
                let metrics_type = &metric.metrics_type;
                let metrics_id = &metric.id;
                let metrics_attribute_namespace_name = match &metric.attributes.namespace_name {
                    Some(namespace_name) => namespace_name.to_string(),
                    None => "-".to_string(),
                };
                let metrics_attribute_table_name = match &metric.attributes.table_name {
                    Some(table_name) => table_name.to_string(),
                    None => "-".to_string(),
                };

                /*
                  the actual statistics are in a vec/list called 'metrics'. That is what is parsed here.
                  the interesting bit is there are two types the randomly are encountered when parsing the metrics:
                  - value type: contain 'name' and 'value'.
                  - latency type: contain 'name', 'total_count', 'total_sum', 'min', 'mean', 'max' and 'percentile_75', 'percentile_95', 'percentile_99', 'percentile_99_9', percentile_99_99'.
                  These statistics are inserted in a nested hashtable in the following way:
                  - hostname:port > metrics_type > metrics_id > statistic name: {metrics/statistics without the name}
                  With an additional caveat: for the 'value', 'total_count' and 'total_sum' the current measurement goes into a field which has 'current_' as prefix.
                  If there is a value in 'current', it is moved to a field with the prefix 'previous_'. That way we can calculate the difference between two measurements.
                */
                for statistic in &metric.metrics {
                    match statistic {
                        NamedMetrics::MetricValue { name, value } => {
                            if *value > 0 {
                                value_statistics.entry(hostname.to_string().into()).or_insert(BTreeMap::new());
                                match value_statistics.get_mut(&hostname.to_string()) {
                                    None => { panic!("value_statistics 1. hostname not found, should have been inserted") }
                                    Some(vs_hostname) => {
                                        vs_hostname.entry(metrics_type.to_string().into()).or_insert(BTreeMap::new());
                                        match vs_hostname.get_mut(&metrics_type.to_string()) {
                                            None => { panic!("value_statistics 2. type not found, should have been inserted")}
                                            Some (vs_type) => {
                                                vs_type.entry( metrics_id.to_string().into()).or_insert(BTreeMap::new());
                                                match vs_type.get_mut( &metrics_id.to_string()) {
                                                    None => { panic!("value_statistics 3. id not found, should have been inserted")}
                                                    Some (vs_id) => {
                                                        match vs_id.get_mut(&name.to_string()) {
                                                            None => {
                                                                // value_statistics 4. name
                                                                vs_id.insert(name.to_string(), XValues { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: fetch_time, previous_time: previous_fetch_time, current_value: *value, previous_value: 0 });
                                                            }
                                                            Some(vs_name) => {
                                                                *vs_name = XValues { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: fetch_time, previous_time: vs_name.current_time, current_value: *value, previous_value: vs_name.current_value };
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            };
                        },
                        NamedMetrics::MetricLatency { name, total_count, min, mean, percentile_75, percentile_95, percentile_99, percentile_99_9, percentile_99_99, max, total_sum } => {
                            if *total_count > 0 {
                                latency_statistics.entry(hostname.to_string().into()).or_insert(BTreeMap::new());
                                match latency_statistics.get_mut(&hostname.to_string()) {
                                    None => { panic!("latency_statistics - hostname not found, should have been inserted") }
                                    Some(ls_hostname) => {
                                        ls_hostname.entry(metrics_type.to_string().into()).or_insert(BTreeMap::new());
                                        match ls_hostname.get_mut(&metrics_type.to_string()) {
                                            None => { panic!("latency_statistics - hostname.type not found, should have been inserted")}
                                            Some (ls_type) => {
                                                ls_type.entry( metrics_id.to_string().into()).or_insert(BTreeMap::new());
                                                match ls_type.get_mut( &metrics_id.to_string()) {
                                                    None => { panic!("latency_statistics - hostname.type.id not found, should have been inserted")}
                                                    Some (ls_id) => {
                                                        match ls_id.get_mut(&name.to_string()) {
                                                            None => {
                                                                ls_id.insert(name.to_string(), XLatencies { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: fetch_time, previous_time: previous_fetch_time, current_total_count: *total_count, previous_total_count: 0, current_min: *min, current_mean: *mean, current_percentile_75: *percentile_75, current_percentile_95: *percentile_95, current_percentile_99: *percentile_99, current_percentile_99_9: *percentile_99_9, current_percentile_99_99: *percentile_99_99, current_max: *max, current_total_sum: *total_sum, previous_total_sum: 0 });
                                                            }
                                                            Some(ls_name) => {
                                                                *ls_name = XLatencies { table_name: metrics_attribute_table_name.to_string(), namespace: metrics_attribute_namespace_name.to_string(), current_time: fetch_time, previous_time: ls_name.current_time, current_total_count: *total_count, previous_total_count: ls_name.current_total_count, current_min: *min, current_mean: *mean, current_percentile_75: *percentile_75, current_percentile_95: *percentile_95, current_percentile_99: *percentile_99, current_percentile_99_9: *percentile_99_9, current_percentile_99_99: *percentile_99_99, current_max: *max, current_total_sum: *total_sum, previous_total_sum: ls_name.current_total_sum };
                                                            }
                                                        };
                                                    }
                                                };
                                            }
                                        };
                                    }
                                };
                            };
                        }
                    };
                };
            };
        };

        if ! begin_end_mode { std::process::Command::new("clear").status().unwrap(); };

        for (hostname_key, hostname_value) in value_statistics.iter() {
            for (type_key, type_value) in hostname_value.iter() {
                for (id_key,  id_value) in type_value.iter() {
                    for (name_key, name_value) in id_value.iter().filter(|(k,_v)| stat_name_filter.is_match(k)) {
                        //if name_value.current_value - name_value.previous_value != 0
                        if name_value.current_time.duration_since(name_value.previous_time).unwrap().as_millis() != 0
                        && table_name_filter.is_match(&name_value.table_name) {
                            let details = match value_statistic_details_lookup.get(&name_key.to_string()) {
                                None => { ValueStatisticDetails { unit: String::from('?'), unit_suffix: String::from('?'), stat_type: String::from('?') }},
                                Some(x) => { ValueStatisticDetails { unit: x.unit.to_string(), unit_suffix: x.unit_suffix.to_string(), stat_type: x.stat_type.to_string() }  }
                            } ;
                            let adaptive_length = if id_key.len() < 15 { 0 }  else { id_key.len()-15 };
                            if details.stat_type == "counter" {
                                if name_value.current_value - name_value.previous_value != 0 {
                                    println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:>15.3}/s",
                                             hostname_key,
                                             type_key,
                                             id_key.substring(adaptive_length, id_key.len()),
                                             name_value.namespace,
                                             name_value.table_name,
                                             name_key,
                                             name_value.current_value - name_value.previous_value,
                                             details.unit_suffix,
                                             ((name_value.current_value - name_value.previous_value) as f64 / (name_value.current_time.duration_since(name_value.previous_time).unwrap().as_millis() as f64) * 1000 as f64),
                                    );
                                };
                            } else {
                                if gauges_enable {
                                    println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:6} {:+15}",
                                             hostname_key,
                                             type_key,
                                             id_key.substring(adaptive_length, id_key.len()),
                                             name_value.namespace,
                                             name_value.table_name,
                                             name_key,
                                             name_value.current_value,
                                             details.unit_suffix,
                                             name_value.current_value - name_value.previous_value
                                    );
                                };
                            };
                        };
                    };
                };
            };
        };
        for (hostname_key, hostname_value) in latency_statistics.iter() {
            for (type_key, type_value) in hostname_value.iter() {
                for (id_key, id_value) in type_value.iter() {
                    for (name_key, name_value) in id_value.iter().filter(|(k,_v)| stat_name_filter.is_match(k)) {
                        if name_value.current_total_count - name_value.previous_total_count != 0
                            && name_value.current_time.duration_since(name_value.previous_time).unwrap().as_millis() != 0
                            && table_name_filter.is_match(&name_value.table_name) {
                            let details = match latency_statistic_details_lookup.get(&name_key.to_string()) {
                                 None => { LatencyStatisticDetails { unit: String::from('?'), unit_suffix: String::from('?'), divisor: 1, stat_type: String::from('?') }},
                                 Some(x) => { LatencyStatisticDetails { unit: x.unit.to_string(), unit_suffix: x.unit_suffix.to_string(), divisor: x.divisor, stat_type: x.stat_type.to_string() }  }
                            } ;
                            let adaptive_length = if id_key.len() < 15 { 0 } else { id_key.len()-15 };
                            println!("{:20} {:8} {:15} {:15} {:30} {:70} {:15} {:>15.3}/s avg: {:>9.0} {:10}",
                                      hostname_key,
                                      type_key,
                                      id_key.substring(adaptive_length,id_key.len()),
                                      name_value.namespace,
                                      name_value.table_name,
                                      name_key,
                                      name_value.current_total_count-name_value.previous_total_count,
                                      ((name_value.current_total_count-name_value.previous_total_count) as f64 / (name_value.current_time.duration_since(name_value.previous_time).unwrap().as_millis() as f64) *100 as f64),
                                      ((name_value.current_total_sum-name_value.previous_total_sum) / (name_value.current_total_count-name_value.previous_total_count)) as f64,
                                      details.unit_suffix
                            );
                        };
                    };
                };
            };
        };

        first_pass = false;
        previous_fetch_time = fetch_time;

        if begin_end_mode {
            if bail_out {
                std::process::exit(0);
            } else {
                bail_out = true;
                println!("Begin metrics snapshot created, press enter to create end snapshot for difference calculation.");
                let mut input = String::new();
                stdin().read_line(&mut input).ok().expect("failed");
            };
        } else {
            std::thread::sleep(std::time::Duration::from_secs(wait_time));
        };
   };
}