use chrono::{DateTime, Local};
use port_scanner::scan_port_addr;
use regex::Regex;


#[derive(Debug)]
pub struct GFlag {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct StoredGFlags {
    pub hostname_port: String,
    pub timestamp: DateTime<Local>,
    pub gflag_name: String,
    pub gflag_value: String,
}


pub fn read_gflags( hostname: &str) -> Vec<GFlag> {
    if ! scan_port_addr( hostname ) {
        println!("Warning: hostname:port {} cannot be reached, skipping", hostname.to_string());
        return Vec::new(); //String::from(""); //Vec::new() //parse_statements(String::from(""))
    }
    if let Ok(data_from_http) = reqwest::blocking::get(format!("http://{}/varz?raw", hostname.to_string())) {
        parse_gflags(data_from_http.text().unwrap())
    } else {
        parse_gflags(String::from(""))
    }
}


pub fn add_to_gflags_vector(gflagdata: Vec<GFlag>,
                            hostname: &str,
                            snapshot_time: DateTime<Local>,
                            stored_gflags: &mut Vec<StoredGFlags>
) {
    for gflag in gflagdata {
        stored_gflags.push( StoredGFlags {
            hostname_port: hostname.to_string(),
            timestamp: snapshot_time,
            gflag_name: gflag.name.to_string(),
            gflag_value: gflag.value.to_string()
        });
    }
}

fn parse_gflags( gflags_data: String ) -> Vec<GFlag> {
    let mut gflags: Vec<GFlag> = Vec::new();
    let re = Regex::new( r"--([A-Za-z_0-9]*)=(.*)\n" ).unwrap();
    for captures in re.captures_iter(&gflags_data) {
        gflags.push(GFlag { name: captures.get(1).unwrap().as_str().to_string(), value: captures.get(2).unwrap().as_str().to_string() });
    }
    gflags
}

