use yb_stats::Metrics;
use std::process;

pub fn parse_metrics( metrics_data: String ) -> Vec<Metrics> {
    serde_json::from_str(&metrics_data)
        .unwrap_or_else(|e| {
            eprintln!("Error parsing response: {}", e);
            process::exit(1);
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_master_2_11_1_0_build_305() {
        let master_metrics = include_str!("master_metrics_2_11_1_0_build_305.json");
        let metrics_parse: serde_json::Result<Vec<Metrics>> = serde_json::from_str(&master_metrics);
        let metrics_parse = metrics_parse.unwrap();
        assert_eq!(metrics_parse.len(),4);
    }
    #[test]
    fn parse_tserver_2_11_1_0_build_305() {
        let tserver_metrics = include_str!("tserver_metrics_2_11_1_0_build_305.json");
        let metrics_parse: serde_json::Result<Vec<Metrics>> = serde_json::from_str(&tserver_metrics);
        let metrics_parse = metrics_parse.unwrap();
        assert_eq!(metrics_parse.len(),6);
    }
}