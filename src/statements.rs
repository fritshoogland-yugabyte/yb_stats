use port_scanner::scan_port_addr;
use chrono::{DateTime, Local};
use serde_derive::{Serialize,Deserialize};


#[derive(Serialize, Deserialize, Debug)]
pub struct Statement {
    pub statements: Vec<Queries>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Queries {
    pub query: String,
    pub calls: i64,
    pub total_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub mean_time: f64,
    pub stddev_time: f64,
    pub rows: i64,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct StoredStatements {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub query: String,
    pub calls: i64,
    pub total_time: f64,
    pub min_time: f64,
    pub max_time: f64,
    pub mean_time: f64,
    pub stddev_time: f64,
    pub rows: i64,
}


pub fn read_statements( hostname: &str) -> Statement {
    if ! scan_port_addr( hostname ) {
        println!("Warning: hostname:port {} cannot be reached, skipping", hostname.to_string());
        return parse_statements(String::from(""))
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}/statements", hostname.to_string())) {
        parse_statements(data_from_http.text().unwrap())
    } else {
        parse_statements(String::from(""))
    }
}

fn parse_statements( statements_data: String ) -> Statement {
    serde_json::from_str( &statements_data )
        .unwrap_or_else(|_e| {
            return Statement { statements: Vec::<Queries>::new() };
        })
}

pub fn add_to_statements_vector(statementdata: Statement,
                                hostname: &str,
                                snapshot_time: DateTime<Local>,
                                stored_statements: &mut Vec<StoredStatements>
) {
    for statement in statementdata.statements {
        stored_statements.push( StoredStatements {
            hostname_port: hostname.to_string(),
            timestamp: snapshot_time,
            query: statement.query.to_string(),
            calls: statement.calls,
            total_time: statement.total_time,
            min_time: statement.min_time,
            max_time: statement.max_time,
            mean_time: statement.mean_time,
            stddev_time: statement.stddev_time,
            rows: statement.rows
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_statements_simple() {
        // This is a very simple example of the statements json.
        let statements_json = r#"{
    "statements": [
        {
            "query": "select count(*) from ybio1.benchmark_table",
            "calls": 1,
            "total_time": 13.76067,
            "min_time": 13.76067,
            "max_time": 13.76067,
            "mean_time": 13.76067,
            "stddev_time": 0.0,
            "rows": 1
        },
        {
            "query": "select $1+$2",
            "calls": 1,
            "total_time": 0.006206000000000001,
            "min_time": 0.006206000000000001,
            "max_time": 0.006206000000000001,
            "mean_time": 0.006206000000000001,
            "stddev_time": 0.0,
            "rows": 1
        }
    ]
}"#.to_string();
        let result = parse_statements(statements_json.clone());
        assert_eq!(result.statements.len(), 2);
    }
}