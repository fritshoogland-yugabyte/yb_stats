//! The  impls and functions
//!
use std::{sync::mpsc::channel, time::Instant};
use chrono::{DateTime, Local};
use regex::Regex;
use log::*;
use anyhow::Result;
use crate::{metrics, utility};
use crate::snapshot;
use crate::metrics::{Metrics::{MetricValue, MetricCountSum, MetricCountSumRows}, MetricEntity, AllMetricEntity, MetricEntityDiff, MetricDiffValues, Attributes, MetricDiffCountSum, MetricDiffCountSumRows};

impl AllMetricEntity {
    pub fn new() -> Self {
        Default::default()
    }
    pub async fn perform_snapshot(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        snapshot_number: i32,
        parallel: usize,
    ) -> Result<()>
    {
        info!("begin snapshot");
        let timer = Instant::now();

        let allmetricentity = AllMetricEntity::read_metrics(hosts, ports, parallel).await;
        snapshot::save_snapshot_json(snapshot_number, "metrics", allmetricentity.metricentity)?;

        info!("end snapshot: {:?}", timer.elapsed());
        Ok(())
    }
    async fn read_metrics(
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
    ) -> AllMetricEntity
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
                        let mut metrics = AllMetricEntity::read_http(host, port);
                        metrics.iter_mut().for_each(|r| r.hostname_port = Some(format!("{}:{}", host, port)));
                        metrics.iter_mut().for_each(|r| r.timestamp = Some(detail_snapshot_time));
                        tx.send(metrics).expect("error sending data via tx");
                    });
                }
            }
        });

        info!("end parallel http read {:?}", timer.elapsed());

        let mut allmetricentity = AllMetricEntity::new();

        for entities in rx
        {
            for entity in entities
            {
                allmetricentity.metricentity.push(entity);
            }
        }

        allmetricentity
    }
    pub fn read_http(
        host: &str,
        port: &str,
    ) -> Vec<MetricEntity>
    {
        let data_from_http = utility::http_get(host, port, "metrics");
        AllMetricEntity::parse_metrics(data_from_http, host, port)
    }
    fn parse_metrics(
        http_data: String,
        host: &str,
        port: &str,
    ) -> Vec<MetricEntity>
    {
        serde_json::from_str(&http_data)
            .unwrap_or_else(|e| {
                debug!("Could not parse {}:{}/metrics, error: {}", host, port, e);
                Vec::<MetricEntity>::new()
            })
    }
}

impl MetricEntityDiff {
    pub fn new() -> Self { Default::default() }
    pub fn snapshot_diff(
        begin_snapshot: &String,
        end_snapshot: &String,
        begin_snapshot_time: &DateTime<Local>,
        details_enable: &bool,
    ) -> Result<MetricEntityDiff>
    {
        let mut metricentitydiff = MetricEntityDiff::new();

        let mut allmetricentity = AllMetricEntity::new();
        allmetricentity.metricentity = snapshot::read_snapshot_json(begin_snapshot, "metrics")?;
        metricentitydiff.first_snapshot(allmetricentity, details_enable);

        let mut allmetricentity = AllMetricEntity::new();
        allmetricentity.metricentity = snapshot::read_snapshot_json(end_snapshot, "metrics")?;
        metricentitydiff.second_snapshot(allmetricentity, details_enable, begin_snapshot_time);

        Ok(metricentitydiff)
    }
    fn first_snapshot(
        &mut self,
        allmetricentity: AllMetricEntity,
        details_enable: &bool,
    )
    {
        for metricentity in allmetricentity.metricentity
        {
            for metric in metricentity.metrics
            {
                match metric
                {
                    MetricValue { name, value } =>
                        {
                            let changed_metrics_id = if !*details_enable
                                && (metricentity.metrics_type.clone() == "table"
                                || metricentity.metrics_type.clone() == "tablet"
                                || metricentity.metrics_type.clone() == "cdc"
                                || metricentity.metrics_type.clone() == "cdcsdk")
                            {
                                "-".to_string()
                            } else {
                                metricentity.id.clone()
                            };
                            self.btreemetricdiffvalue
                                .entry((
                                    metricentity.hostname_port
                                        .clone()
                                        .expect("hostname:port should be set"),
                                    metricentity.metrics_type
                                        .clone(),
                                    changed_metrics_id,
                                    name.clone()
                                ))
                                .and_modify(|row| {
                                    if *details_enable
                                        && (metricentity.metrics_type.clone() == "table"
                                        || metricentity.metrics_type.clone() == "tablet"
                                        || metricentity.metrics_type.clone() == "cdc"
                                        || metricentity.metrics_type.clone() == "cdcsdk")
                                    {
                                        row.first_value += value;
                                    } else {
                                        warn!("Duplicate entry: hostname_port: {}, metrics_type: {}, id: {}, name: {}",
                                            metricentity.hostname_port
                                                .clone()
                                                .expect("hostname:port should be set"),
                                            metricentity.metrics_type
                                                .clone(),
                                            metricentity.id
                                                .clone(),
                                            name.clone()
                                        );
                                    };
                                })
                                .or_insert(MetricDiffValues {
                                    table_name: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .table_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    namespace: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .namespace_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    first_snapshot_time: metricentity.timestamp
                                        .unwrap_or_default(),
                                    first_value: value,
                                    ..Default::default()
                                });
                        }
                    MetricCountSum { name, total_count, total_sum, .. } =>
                        {
                            let changed_metrics_id = if !*details_enable
                                && (metricentity.metrics_type.clone() == "table"
                                || metricentity.metrics_type.clone() == "tablet"
                                || metricentity.metrics_type.clone() == "cdc"
                                || metricentity.metrics_type.clone() == "cdcsdk")
                            {
                                "-".to_string()
                            } else {
                                metricentity.id.clone()
                            };
                            self.btreemetricdiffcountsum
                                .entry((
                                    metricentity.hostname_port
                                        .clone()
                                        .expect("hostname:port should be set"),
                                    metricentity.metrics_type
                                        .clone(),
                                    changed_metrics_id,
                                    name.clone()
                                ))
                                .and_modify(|row| {
                                    if *details_enable
                                        && (metricentity.metrics_type.clone() == "table"
                                        || metricentity.metrics_type.clone() == "tablet"
                                        || metricentity.metrics_type.clone() == "cdc"
                                        || metricentity.metrics_type.clone() == "cdcsdk")
                                    {
                                        row.first_total_count += total_count;
                                        row.first_total_sum += total_sum;
                                    } else {
                                        warn!("Duplicate entry: hostname_port: {}, metrics_type: {}, id: {}, name: {}",
                                            metricentity.hostname_port
                                                .clone()
                                                .expect("hostname:port should be set"),
                                            metricentity.metrics_type
                                                .clone(),
                                            metricentity.id
                                                .clone(),
                                            name.clone()
                                        );
                                    };
                                })
                                .or_insert(MetricDiffCountSum {
                                    table_name: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .table_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    namespace: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .namespace_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    first_snapshot_time: metricentity.timestamp
                                        .unwrap_or_default(),
                                    first_total_count: total_count,
                                    first_total_sum: total_sum,
                                    ..Default::default()
                                });
                        }
                    MetricCountSumRows { name, count, sum, rows, .. } =>
                        {
                            self.btreemetricdiffcountsumrows
                                .entry((
                                    metricentity.hostname_port
                                        .clone()
                                        .expect("hostname:port should be set"),
                                    metricentity.metrics_type
                                        .clone(),
                                    metricentity.id
                                        .clone(),
                                    name.clone()
                                ))
                                .and_modify(|_| {
                                    warn!("Duplicate entry: hostname_port: {}, metrics_type: {}, id: {}, name: {}",
                                            metricentity.hostname_port
                                                .clone()
                                                .expect("hostname:port should be set"),
                                            metricentity.metrics_type
                                                .clone(),
                                            metricentity.id
                                                .clone(),
                                            name.clone()
                                        );
                                })
                                .or_insert(MetricDiffCountSumRows {
                                    table_name: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .table_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    namespace: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .namespace_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    first_snapshot_time: metricentity.timestamp
                                        .unwrap_or_default(),
                                    first_count: count,
                                    first_sum: sum,
                                    first_rows: rows,
                                    ..Default::default()
                                });
                        }
                    _ =>
                        {
                            debug!("Encountered unknown metric type: {:?}", metric);
                        }
                }
            }
        }
    }
    fn second_snapshot(
        &mut self,
        allmetricentity: AllMetricEntity,
        details_enable: &bool,
        begin_snapshot_timestamp: &DateTime<Local>,
    )
    {
        for metricentity in allmetricentity.metricentity
        {
            for metric in metricentity.metrics
            {
                match metric
                {
                    MetricValue { name, value } =>
                        {
                            let changed_metrics_id = if !*details_enable
                                && (metricentity.metrics_type.clone() == "table"
                                || metricentity.metrics_type.clone() == "tablet"
                                || metricentity.metrics_type.clone() == "cdc"
                                || metricentity.metrics_type.clone() == "cdcsdk")
                            {
                                "-".to_string()
                            } else {
                                metricentity.id.clone()
                            };
                            self.btreemetricdiffvalue
                                .entry((
                                    metricentity.hostname_port
                                        .clone()
                                        .expect("hostname:port should be set"),
                                    metricentity.metrics_type
                                        .clone(),
                                    changed_metrics_id,
                                    name.clone(),
                                ))
                                .and_modify(|row| {
                                    row.second_snapshot_time = metricentity.timestamp.unwrap_or_default();
                                    row.second_value += value;
                                })
                                .or_insert(MetricDiffValues {
                                    table_name: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .table_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    namespace: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .namespace_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    first_snapshot_time: *begin_snapshot_timestamp,
                                    second_snapshot_time: metricentity.timestamp
                                        .unwrap_or_default(),
                                    second_value: value,
                                    ..Default::default()
                                });
                        }
                    MetricCountSum { name, total_count, total_sum, .. } =>
                        {
                            let changed_metrics_id = if !*details_enable
                                && (metricentity.metrics_type.clone() == "table"
                                || metricentity.metrics_type.clone() == "tablet"
                                || metricentity.metrics_type.clone() == "cdc"
                                || metricentity.metrics_type.clone() == "cdcsdk")
                            {
                                "-".to_string()
                            } else {
                                metricentity.id.clone()
                            };
                            self.btreemetricdiffcountsum
                                .entry((
                                    metricentity.hostname_port
                                        .clone()
                                        .expect("hostname:port should be set"),
                                    metricentity.metrics_type
                                        .clone(),
                                    changed_metrics_id,
                                    name.clone()
                                ))
                                .and_modify(|row| {
                                    row.second_snapshot_time = metricentity.timestamp.unwrap_or_default();
                                    row.second_total_count += total_count;
                                    row.second_total_sum += total_sum;
                                })
                                .or_insert(MetricDiffCountSum {
                                    table_name: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .table_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    namespace: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .namespace_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    first_snapshot_time: *begin_snapshot_timestamp,
                                    second_snapshot_time: metricentity.timestamp
                                        .unwrap_or_default(),
                                    second_total_count: total_count,
                                    second_total_sum: total_sum,
                                    ..Default::default()
                                });
                        }
                    MetricCountSumRows { name, count, sum, rows, .. } =>
                        {
                            self.btreemetricdiffcountsumrows
                                .entry((
                                    metricentity.hostname_port
                                        .clone()
                                        .expect("hostname:port should be set"),
                                    metricentity.metrics_type
                                        .clone(),
                                    metricentity.id
                                        .clone(),
                                    name.clone()
                                ))
                                .and_modify(|row| {
                                    row.second_snapshot_time = metricentity.timestamp.unwrap_or_default();
                                    row.second_count = count;
                                    row.second_sum = sum;
                                    row.second_rows = rows;
                                })
                                .or_insert(MetricDiffCountSumRows {
                                    table_name: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .table_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    namespace: metricentity.attributes
                                        .as_ref()
                                        .unwrap_or(&Attributes::default())
                                        .namespace_name
                                        .as_ref()
                                        .unwrap_or(&"".to_string())
                                        .to_string(),
                                    second_snapshot_time: metricentity.timestamp
                                        .unwrap_or_default(),
                                    second_count: count,
                                    second_sum: sum,
                                    second_rows: rows,
                                    ..Default::default()
                                });
                        }
                    _ =>
                        {
                            debug!("Encountered unknown metric type: {:?}", metric);
                        }
                }
            }
        }
    }
    pub async fn print(
        &self,
        hostname_filter: &Regex,
        stat_name_filter: &Regex,
        table_name_filter: &Regex,
        details_enable: &bool,
        gauges_enable: &bool,
    )
    {
        // value_diff
        let value_statistics = metrics::ValueStatistics::create();
        for ((hostname, metric_type, metric_id, metric_name), row) in &self.btreemetricdiffvalue
        {
            let metadata = value_statistics.lookup(metric_name);
            // if second_value = 0, the statistic was zero, meaning no use,
            // or it wasn't filled out for the second snapshot, meaning the endppoint went away.
            // in both cases do not report.
            if row.second_value > 0
                && hostname_filter.is_match(hostname)
                && stat_name_filter.is_match(metric_name)
                && table_name_filter.is_match(&row.table_name)
            {
                // show as counter.
                // the choice of != gauge is deliberate here: if it's unknown, it'll be shown as counter.
                if metadata.stat_type != "gauge"
                    && row.second_value - row.first_value != 0
                {
                    if *details_enable
                    {
                        let table_info = if row.namespace.is_empty() && row.table_name.is_empty()
                        {
                            "".to_string()
                        } else {
                            format!("{}.{}", row.namespace, row.table_name)
                        };

                        //println!("{:20} {:8} {:32} {:15} {:30} {:70} {:15} {:6} {:>15.3} /s",
                        println!("{:20} {:8} {:32} {:30} {:70} {:15} {:6} {:>15.3} /s",
                                 hostname,
                                 metric_type,
                                 metric_id,
                                 table_info,
                                 metric_name,
                                 row.second_value - row.first_value,
                                 metadata.unit_suffix,
                                 ((row.second_value - row.first_value) as f64 / (row.second_snapshot_time - row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64)
                        );
                    } else {
                        println!("{:20} {:8} {:70} {:15} {:6} {:>15.3} /s",
                                 hostname,
                                 metric_type,
                                 metric_name,
                                 row.second_value - row.first_value,
                                 metadata.unit_suffix,
                                 ((row.second_value - row.first_value) as f64 / (row.second_snapshot_time - row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64)
                        );
                    }
                }
                // show as gauge
                // gauges are shown when the difference between first and second snapshot is zero,
                // provided the absolute second value is higher than 0.
                if metadata.stat_type == "gauge"
                    && *gauges_enable
                {
                    if *details_enable
                    {
                        let table_info = if row.namespace.is_empty() && row.table_name.is_empty()
                        {
                            "".to_string()
                        } else {
                            format!("{}.{}", row.namespace, row.table_name)
                        };
                        println!("{:20} {:8} {:32} {:30} {:70} {:15} {:6} {:+15}",
                                 hostname,
                                 metric_type,
                                 metric_id,
                                 table_info,
                                 metric_name,
                                 row.second_value,
                                 metadata.unit_suffix,
                                 row.second_value - row.first_value
                        );
                    }
                    else
                    {
                        println!("{:20} {:8} {:70} {:15} {:6} {:+15}",
                                 hostname,
                                 metric_type,
                                 metric_name,
                                 row.second_value,
                                 metadata.unit_suffix,
                                 row.second_value - row.first_value
                        );
                    }
                }
            }
        }
        // countsum_diff
        let countsum_statistics = metrics::CountSumStatistics::create();
        for ((hostname, metric_type, metric_id, metric_name), row) in &self.btreemetricdiffcountsum
        {
            let metadata = countsum_statistics.lookup(metric_name);
            if row.second_total_count > 0
                && hostname_filter.is_match(hostname)
                && stat_name_filter.is_match(metric_name)
                && table_name_filter.is_match(&row.table_name)
            {
                // if second_total_count = 0, the statistic was zero, meaning no use,
                // or it wasn't filled out for the second snapshot, meaning the endpoint went away.
                // in both cases do not report.
                if row.second_total_count - row.first_total_count != 0
                {
                    if *details_enable
                    {
                        let table_info = if row.namespace.is_empty() && row.table_name.is_empty()
                        {
                            "".to_string()
                        } else {
                            format!("{}.{}", row.namespace, row.table_name)
                        };
                        println!("{:20} {:8} {:32} {:30} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
                                 hostname,
                                 metric_type,
                                 metric_id,
                                 table_info,
                                 metric_name,
                                 row.second_total_count - row.first_total_count,
                                 (row.second_total_count - row.first_total_count) as f64 / (row.second_snapshot_time - row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64,
                                 ((row.second_total_sum - row.first_total_sum) / (row.second_total_count - row.first_total_count)) as f64,
                                 row.second_total_sum - row.first_total_sum,
                                 metadata.unit_suffix
                        );
                    }
                    else
                    {
                        println!("{:20} {:8} {:70} {:15}        {:>15.3} /s avg: {:9.0} tot: {:>15.3} {:10}",
                                 hostname,
                                 metric_type,
                                 metric_name,
                                 row.second_total_count - row.first_total_count,
                                 (row.second_total_count - row.first_total_count) as f64 / (row.second_snapshot_time - row.first_snapshot_time).num_milliseconds() as f64 * 1000_f64,
                                 ((row.second_total_sum - row.first_total_sum) / (row.second_total_count - row.first_total_count)) as f64,
                                 row.second_total_sum - row.first_total_sum,
                                 metadata.unit_suffix
                        );
                    }
                }
            }
        }
        // countsumrows_diff
        for ((hostname, _metric_type, _metric_id, metric_name), row) in &self.btreemetricdiffcountsumrows
        {
            if hostname_filter.is_match(hostname)
                && stat_name_filter.is_match(metric_name)
                && row.second_count - row.first_count != 0
            {
                println!("{:20} {:70} {:>15} avg: {:>15.3} tot: {:>15.3} ms, avg: {:>15} tot: {:>15} rows",
                         hostname,
                         metric_name,
                         row.second_count - row.first_count,
                         ((row.second_sum as f64 - row.first_sum as f64) / 1000.0) / (row.second_count - row.first_count) as f64,
                         (row.second_sum as f64 - row.first_sum as f64) / 1000.0,
                         (row.second_rows - row.first_rows) / (row.second_count - row.first_count),
                         row.second_rows - row.first_rows
                );
            }
        }
    }
    pub async fn adhoc_read_first_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
        details_enable: bool,
    )
    {
        let allmetricentity = AllMetricEntity::read_metrics(hosts, ports, parallel).await;
        self.first_snapshot(allmetricentity, &details_enable);
    }
    pub async fn adhoc_read_second_snapshot(
        &mut self,
        hosts: &Vec<&str>,
        ports: &Vec<&str>,
        parallel: usize,
        first_snapshot_time: &DateTime<Local>,
        details_enable: bool,
    )
    {
        let allmetricentity = AllMetricEntity::read_metrics(hosts, ports, parallel).await;
        self.second_snapshot(allmetricentity, &details_enable, first_snapshot_time);
    }
}

#[cfg(test)]
mod tests {
    use crate::metrics::Metrics::{RejectedBooleanMetricValue, RejectedU64MetricValue};
    use super::*;

    #[test]
    /// cdcsdk (change data capture software development kit) metrics value
    /// Please mind type cdc has an extra, unique, attribute: stream_id. This is currently not parsed.
    fn unit_parse_metrics_cdcsdk_value() {
        let json = r#"
[
    {
        "type": "cdcsdk",
        "id": ":face4edb05934e77b564857878cf5015:4457a26b28a64393ac626504aba5f571",
        "attributes": {
            "stream_id": "face4edb05934e77b564857878cf5015",
            "table_name": "table0",
            "namespace_name": "test",
            "table_id": "c70ffbbe28f14e84b0559c405ae20197"
        },
        "metrics": [
            {
                "name": "async_replication_sent_lag_micros",
                "value": 0
            }
        ]
    }
]"#.to_string();
        let result = AllMetricEntity::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"cdcsdk");
        match &result[0].metrics[0] {
            MetricValue { name, value} =>
                {
                    assert_eq!(name.clone(), "async_replication_sent_lag_micros");
                    assert_eq!(value, &0);
                },
            _ => {}
        };
    }

    #[test]
    /// cdc (change data capture) metrics value
    /// Please mind type cdc has an extra, unique, attribute: stream_id. This is currently not parsed.
    fn unit_parse_metrics_cdc_value() {
        let json = r#"
[
    {
        "type": "cdc",
        "id": ":face4edb05934e77b564857878cf5015:4457a26b28a64393ac626504aba5f571",
        "attributes": {
            "stream_id": "face4edb05934e77b564857878cf5015",
            "table_name": "table0",
            "namespace_name": "test",
            "table_id": "c70ffbbe28f14e84b0559c405ae20197"
        },
        "metrics": [
            {
                "name": "async_replication_sent_lag_micros",
                "value": 0
            }
        ]
    }
]"#.to_string();
        let result = AllMetricEntity::parse_metrics(json, "", "");
        assert_eq!(result[0].metrics_type,"cdc");
        match &result[0].metrics[0] {
            MetricValue { name, value} =>
                {
                    assert_eq!(name.clone(), "async_replication_sent_lag_micros");
                    assert_eq!(value, &0);
                },
            _ => {}
        };
    }

        #[test]
        /// cdc (change data capture) metrics value
        /// Please mind type cdc has an extra, unique, attribute: stream_id. This is currently not parsed.
        fn unit_parse_metrics_cdc_countsum() {
            let json = r#"
    [
        {
            "type": "cdc",
            "id": ":face4edb05934e77b564857878cf5015:4457a26b28a64393ac626504aba5f571",
            "attributes": {
                "stream_id": "face4edb05934e77b564857878cf5015",
                "table_name": "table0",
                "namespace_name": "test",
                "table_id": "c70ffbbe28f14e84b0559c405ae20197"
            },
            "metrics": [
                {
                    "name": "rpc_payload_bytes_responded",
                    "total_count": 3333,
                    "min": 0,
                    "mean": 0.0,
                    "percentile_75": 0,
                    "percentile_95": 0,
                    "percentile_99": 0,
                    "percentile_99_9": 0,
                    "percentile_99_99": 0,
                    "max": 0,
                    "total_sum": 4444
                }
            ]
        }
    ]"#.to_string();
            let result = AllMetricEntity::parse_metrics(json, "", "");
            assert_eq!(result[0].metrics_type,"cdc");
            match &result[0].metrics[0] {
                MetricCountSum { name, total_count, total_sum, .. } =>
                    {
                        assert_eq!(name.clone(), "rpc_payload_bytes_responded");
                        assert_eq!(total_count, &3333);
                        assert_eq!(total_sum, &4444);
                    },
                _ => {}
            };
        }

        #[test]
        /// Parse tablet metrics value.
        /// It seems there are no other types of metrics in tablet metrics.
        fn unit_parse_metrics_tablet_value() {
            let json = r#"
    [
        {
            "type": "tablet",
            "id": "16add7b1248a45d2880e5527b2059b54",
            "attributes": {
                "namespace_name": "yugabyte",
                "table_name": "config",
                "table_id": "000033e10000300080000000000042d9"
            },
            "metrics": [
                {
                    "name": "rocksdb_sequence_number",
                    "value": 1125899906842624
                }
            ]
        }
    ]"#.to_string();
            let result = AllMetricEntity::parse_metrics(json, "", "");
            assert_eq!(result[0].metrics_type,"tablet");
            match &result[0].metrics[0] {
                MetricValue { name, value} =>
                    {
                        assert_eq!(name.clone(), "rocksdb_sequence_number");
                        assert_eq!(value, &1125899906842624);
                    },
                _ => {}
            };
        }

        #[test]
        /// Parse table metrics countsum
        /// It seems tables can have both countsum as well as value metrics
        fn unit_parse_metrics_table_countsum() {
            let json = r#"
    [
        {
            "type": "table",
            "id": "000033e10000300080000000000042ac",
            "attributes": {
                "namespace_name": "yugabyte",
                "table_name": "benchmark_table",
                "table_id": "000033e10000300080000000000042ac"
            },
            "metrics": [
                {
                    "name": "log_sync_latency",
                    "total_count": 21,
                    "min": 0,
                    "mean": 0.0,
                    "percentile_75": 0,
                    "percentile_95": 0,
                    "percentile_99": 0,
                    "percentile_99_9": 0,
                    "percentile_99_99": 0,
                    "max": 0,
                    "total_sum": 22349
                }
            ]
        }
    ]"#.to_string();
            let result = AllMetricEntity::parse_metrics(json, "", "");
            assert_eq!(result[0].metrics_type,"table");
            match &result[0].metrics[0] {
                MetricCountSum { name, total_count, total_sum, .. } =>
                    {
                        assert_eq!(name.clone(), "log_sync_latency");
                        assert_eq!(total_count, &21);
                        assert_eq!(total_sum, &22349);
                    },
                _ => {}
            };
        }

        #[test]
        /// Parse table metrics countsum
        /// It seems tables can have both countsum as well as value metrics
        fn unit_parse_metrics_table_value() {
            let json = r#"
    [
        {
            "type": "table",
            "id": "sys.catalog.uuid",
            "attributes": {
                "table_name": "sys.catalog",
                "namespace_name": "",
                "table_id": "sys.catalog.uuid"
            },
            "metrics": [
                {
                    "name": "log_gc_running",
                    "value": 0
                }
            ]
        }
    ]"#.to_string();
            let result = AllMetricEntity::parse_metrics(json, "", "");
            assert_eq!(result[0].metrics_type,"table");
            match &result[0].metrics[0] {
                MetricValue { name, value} =>
                    {
                        assert_eq!(name.clone(), "log_gc_running");
                        assert_eq!(value, &0);
                    },
                _ => {}
            };
        }

        #[test]
        /// Parse cluster metrics value
        fn unit_parse_metrics_cluster_value() {
            let json = r#"
    [
            {
            "type": "cluster",
            "id": "yb.cluster",
            "attributes": {},
            "metrics": [
               {
                    "name": "num_tablet_servers_live",
                    "value": 0
                }
            ]
        }
    ]"#.to_string();
            let result = AllMetricEntity::parse_metrics(json, "", "");
            assert_eq!(result[0].metrics_type,"cluster");
            match &result[0].metrics[0] {
                MetricValue { name, value} =>
                    {
                        assert_eq!(name.clone(), "num_tablet_servers_live");
                        assert_eq!(value, &0);
                    },
                _ => {}
            };
        }

        #[test]
        /// Parse server metrics value
        fn unit_parse_metrics_server_value() {
            let json = r#"
    [
        {
            "type": "server",
            "id": "yb.master",
            "attributes": {},
            "metrics": [
                {
                    "name": "mem_tracker",
                    "value": 529904
                }
            ]
        }
    ]"#.to_string();
            let result = AllMetricEntity::parse_metrics(json, "", "");
            assert_eq!(result[0].metrics_type,"server");
            match &result[0].metrics[0] {
                MetricValue { name, value} =>
                    {
                        assert_eq!(name.clone(), "mem_tracker");
                        assert_eq!(value, &529904);
                    },
                _ => {}
            }
        }

        #[test]
        /// Parse server metrics countsum
        fn unit_parse_metrics_server_countsum() {
            let json = r#"
    [
        {
            "type": "server",
            "id": "yb.tabletserver",
            "attributes": {},
            "metrics": [
                {
                    "name": "handler_latency_outbound_call_time_to_response",
                    "total_count": 1384630,
                    "min": 0,
                    "mean": 676.4016688575184,
                    "percentile_75": 2,
                    "percentile_95": 2,
                    "percentile_99": 2,
                    "percentile_99_9": 2,
                    "percentile_99_99": 2,
                    "max": 25000,
                    "total_sum": 1057260382
                }
            ]
        }
    ]"#.to_string();
            let result = AllMetricEntity::parse_metrics(json, "", "");
            assert_eq!(result[0].metrics_type,"server");
            match &result[0].metrics[0] {
                MetricCountSum { name, total_count, total_sum, .. } =>
                    {
                        assert_eq!(name.clone(), "handler_latency_outbound_call_time_to_response");
                        assert_eq!(total_count, &1384630);
                        assert_eq!(total_sum, &1057260382);
                    },
                _ => {}
            };
        }

        #[test]
        /// Parse YSQL server countsumrows
        /// The YSQL metrics are unique metrics
        fn unit_parse_metrics_server_countsumrows() {
            let json = r#"
    [
        {
            "type": "server",
            "id": "yb.ysqlserver",
            "metrics": [
                {
                    "name": "handler_latency_yb_ysqlserver_SQLProcessor_CatalogCacheMisses",
                    "count": 439,
                    "sum": 0,
                    "rows": 439
                }
            ]
        }
    ]"#.to_string();
            let result = AllMetricEntity::parse_metrics(json, "", "");
            assert_eq!(result[0].metrics_type,"server");
            match &result[0].metrics[0] {
                MetricCountSumRows { name, count, sum, rows } =>
                    {
                        assert_eq!(name.clone(), "handler_latency_yb_ysqlserver_SQLProcessor_CatalogCacheMisses");
                        assert_eq!(count, &439);
                        assert_eq!(sum, &0);
                        assert_eq!(rows, &439);
                    },
                _ => {}
            };
        }

        #[test]
        fn unit_parse_metrics_server_rejectedu64metricvalue() {
            // Funny, when I checked with version 2.11.2.0-b89 I could not find the value that only fitted in an unsigned 64 bit integer.
            // Still let's check for it.
            // The id is yb.cqlserver, because that is where I found this value.
            // The value 18446744073709551615 is too big for a signed 64 bit integer (limit = 2^63-1), this value is 2^64-1.
            let json = r#"
    [
        {
            "type": "server",
            "id": "yb.cqlserver",
            "attributes": {},
            "metrics":
            [
                {
                    "name": "madeup_value",
                    "value": 18446744073709551615
                }
            ]
        }
    ]"#.to_string();
            let result = AllMetricEntity::parse_metrics(json, "", "");
            assert_eq!(result[0].metrics_type,"server");
            match &result[0].metrics[0] {
                RejectedU64MetricValue { name, value} =>
                    {
                        assert_eq!(name.clone(), "madeup_value");
                        assert_eq!(value, &18446744073709551615);
                    },
                _ => {}
            };
        }

        #[test]
        fn unit_parse_metrics_cluster_rejectedbooleanmetricvalue() {
            // Version 2.15.2.0-b83 a value appeared that is boolean instead of a number.
            // For now, I will just ditch the value, and request for it to be handled as all the other booleans, which are by the values of 0 and 1.
            let json = r#"
    [
       {
            "type": "cluster",
            "id": "yb.cluster",
            "attributes": {},
            "metrics":
            [
                {
                    "name": "is_load_balancing_enabled",
                    "value": false
                }
            ]
       }
    ]"#.to_string();
            let result = AllMetricEntity::parse_metrics(json, "", "");
            assert_eq!(result[0].metrics_type,"cluster");
            match &result[0].metrics[0] {
                RejectedBooleanMetricValue { name, value} =>
                    {
                        assert_eq!(name.clone(), "is_load_balancing_enabled");
                        assert_eq!(value, &false);
                    },
                _ => {}
            };
        }

        /*
        fn test_function_read_metrics(
            hostname: String,
            port: String
        ) -> AllStoredMetrics
        {
            let mut allstoredmetrics = AllStoredMetrics::new();

            let data_parsed_from_json = AllStoredMetrics::read_http(hostname.as_str(), port.as_str());
            allstoredmetrics.split_into_vectors(data_parsed_from_json, format!("{}:{}", hostname, port).as_str(), Local::now());
            allstoredmetrics
        }

         */
        #[tokio::test]
        async fn integration_parse_metrics_master()
        {
            let hostname = utility::get_hostname_master();
            let port = utility::get_port_master();
            let allmetricentity = AllMetricEntity::read_metrics(&vec![&hostname], &vec![&port], 1).await;
            // a master will produce metrics.
            assert!(!allmetricentity.metricentity.is_empty());
        }
        #[tokio::test]
        async fn integration_parse_metrics_tserver() {
            let hostname = utility::get_hostname_tserver();
            let port = utility::get_port_tserver();
            let allmetricentity = AllMetricEntity::read_metrics(&vec![&hostname], &vec![&port], 1).await;
            // a tablet server will produce metrics.
            assert!(!allmetricentity.metricentity.is_empty());
        }
        #[tokio::test]
        async fn integration_parse_metrics_ysql() {
            let hostname = utility::get_hostname_ysql();
            let port = utility::get_port_ysql();
            let allmetricentity = AllMetricEntity::read_metrics(&vec![&hostname], &vec![&port], 1).await;
            // YSQL will produce metrics.
            assert!(!allmetricentity.metricentity.is_empty());
        }
        #[tokio::test]
        async fn integration_parse_metrics_ycql() {
            let hostname = utility::get_hostname_ycql();
            let port = utility::get_port_ycql();
            let allmetricentity = AllMetricEntity::read_metrics(&vec![&hostname], &vec![&port], 1).await;
            // YCQL will produce metrics.
            assert!(!allmetricentity.metricentity.is_empty());
        }
        #[tokio::test]
        async fn integration_parse_metrics_yedis() {
            let hostname = utility::get_hostname_yedis();
            let port = utility::get_port_yedis();
            let allmetricentity = AllMetricEntity::read_metrics(&vec![&hostname], &vec![&port], 1).await;
            // YEDIS will produce metrics.
            assert!(!allmetricentity.metricentity.is_empty());
        }
}