//! Utility module for the [Snapshot] struct and snapshot CSV file.
//!
//! This currently leaves a single snapshot function in lib.rs which performs the complete snapshot of all modules.
//! Because all the interaction of [Snapshot] is including reading and writing to a CSV file, there are no unittests.
use log::*;
use std::{fs, path::Path, env};
use std::io::{stdin, stdout, Write};
use chrono::{DateTime, Local};
use anyhow::{Context, Result};
use serde::{Serialize, Deserialize};
/// Struct to represent the snapshots in yb_stats in a vector as well as on disk as CSV using serde.
/// The comment can be empty, unless a snapshot is made with the `--snapshot-comment` flag and a comment.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Snapshot {
    pub number: i32,
    pub timestamp: DateTime<Local>,
    pub comment: String,
}

impl Snapshot {
    /// This is a public function to use the stored CSV snapshot file, determine the highest snapshot number and insert a new snapshot with current timestamp with a snapshot number one higher.
    /// If the snapshot file doesn't exist, it will be created (yb_stats.snapshots/snapshot.index).
    /// If the file does exist, the snapshots are read into a vector and the highest snapshot number is determined.
    /// Then a struct is added to the vector, and the file is overwritten with the new vector.
    /// The last things done are: the snapshot directory for the data is created (yb_stats.snapshots/<nr>) and the snapshot number is returned.
    pub fn insert_new_snapshot_number(
        snapshot_comment: Option<String>
    ) -> Result<i32>
    {
        info!("read_snapshot_number");
        let mut snapshots: Vec<Snapshot> = Vec::new();
        let mut snapshot_number: i32 = 0;

        // setup yb_stats.snapshots if necessary
        let current_directory = env::current_dir()
            .with_context(|| "Cannot evaluate current working directory" )?;
        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        // If the &yb_stats_directory does not exist, create it.
        // If it does exist already, nothing happens and continue.
        fs::create_dir_all(&yb_stats_directory)
            .with_context(|| format!("Cannot create directory: {}",&yb_stats_directory.clone().into_os_string().into_string().unwrap()))?;
        // If &yb_stats_directory/snapshot.index exists, read snapshots into snapshots vector,
        // and determine the highest snapshot number, add one and assign it to snapshot_number.
        // If it doesn't exist, snapshot_number 0 is okay.
        let snapshot_index = &yb_stats_directory.join("snapshot.index");
        if Path::new(&snapshot_index).exists() {
            snapshots = Snapshot::read_snapshots()?;
            let record_with_highest_snapshot_number = snapshots.iter().max_by_key(|k| k.number).unwrap();
            snapshot_number = record_with_highest_snapshot_number.number + 1;
        }
        // create a new snapshot vector and assign it the new_snapshot, and add it to the snapshots vector.
        let new_snapshot: Snapshot = Snapshot { number: snapshot_number, timestamp: Local::now(), comment: snapshot_comment.unwrap_or_default() };
        snapshots.push(new_snapshot);
        Snapshot::write_snapshots(snapshots)?;
        // Create the snapshot number directory in the &yb_stats_directory
        let current_snapshot_directory = &yb_stats_directory.join(&snapshot_number.to_string());
        fs::create_dir_all(current_snapshot_directory)
            .with_context(|| format!("Cannot create directory: {}",&current_snapshot_directory.clone().into_os_string().into_string().unwrap()))?;
        Ok(snapshot_number)
    }
    /// This is a private function to read the snapshots file, and return a vector with the snapshots.
    fn read_snapshots(
    ) -> Result<Vec<Snapshot>>
    {
        let mut snapshots: Vec<Snapshot> = Vec::new();
        let current_directory = env::current_dir()
            .with_context(|| "Cannot evaluate current working directory" )?;
        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        let snapshot_index = &yb_stats_directory.join("snapshot.index");

        let file = fs::File::open(snapshot_index)
            .with_context(|| format!("Error opening file: {}", snapshot_index.display()))?;
        let mut reader = csv::Reader::from_reader(file);
        for row in reader.deserialize() {
            let data: Snapshot = row
                .with_context(|| "Error deserialize row")?;
            snapshots.push(data);
        }
        Ok(snapshots)
    }
    /// This is a private function to write the vector to the snapshots file. The file gets truncated and overwritten.
    fn write_snapshots(
        snapshots: Vec<Snapshot>
    ) -> Result<()>
    {
        let current_directory = env::current_dir()
            .with_context(|| "Cannot evaluate current working directory" )?;

        let yb_stats_directory = current_directory.join("yb_stats.snapshots");
        let snapshot_index = &yb_stats_directory.join("snapshot.index");

        // Open the snapshot.index file, but truncate it and write the new snapshots vector to it.
        let file = fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(snapshot_index)
            .with_context(|| format!("Cannot create file: {}", snapshot_index.display()))?;
        let mut writer = csv::Writer::from_writer(file);
        for row in snapshots {
            writer.serialize(&row)
                .with_context(|| format!("Unable to serialize: {} {} {}", row.number, row.timestamp, row.comment))?;
        }
        writer.flush()
            .with_context(|| "Error flushing buffer")?;
        Ok(())
    }
    /// This is a public function that reads the snapshots file into a vector and print the contents of it.
    /// The main use is to display the current snapshots to the user.
    pub fn print(
    ) -> Result<()>
    {
        let snapshots = Snapshot::read_snapshots()?;
        for row in &snapshots {
            println!("{:>3} {:30} {:50}", row.number, row.timestamp, row.comment);
        }
        Ok(())
    }
    /// This is a public function that validates begin and end provided values, and if these are not specified are requested interactively, after which the begin and end snapshot numbers and the struct with the begin snapshot are returned as record.
    /// If the begin or end value is provided (using the switches `-b`/`--begin` and `-e`/`--end`), it will take that value and not ask for it.
    /// Both begin and end snapshots are validated for their existence in the [Snapshot] vector.
    /// Besides the begin and end snapshot values, the struct with the begin [Snapshot] is returned.
    /// The begin [Snapshot] struct is needed for the timestamp.
    pub fn read_begin_end_snapshot_from_user(
        option_begin: Option<i32>,
        option_end: Option<i32>
    ) -> Result<(String, String, Snapshot)>
    {
        let snapshots = Snapshot::read_snapshots()?;
        let begin_snapshot= match option_begin {
            Some(nr) => nr,
            None => {
                print!("Enter begin snapshot: ");
                let mut snap= String::new();
                stdout().flush()?;
                stdin().read_line(&mut snap).expect("Failed to read input.");
                let snap: i32 = snap.trim().parse().expect("Invalid input");
                snap
            }
        };
        // begin_snapshot has to exists as row.number.
        // if it does, assign to begin_snapshot_row, otherwise error out.
        let begin_snapshot_row = snapshots.iter()
            .find(|&row| row.number == begin_snapshot)
            .with_context(|| format!("Unable to find begin snapshot number: {}", begin_snapshot))?;
        // if option_end is filled out (Some), use it.
        // if option_end is not filled out (None), read from stdin.
        let end_snapshot = match option_end {
            Some(nr) => nr,
            None => {
                print!("Enter end snapshot: ");
                let mut snap = String::new();
                stdout().flush()?;
                stdin().read_line(&mut snap).expect("Failed to read input.");
                let snap: i32 = snap.trim().parse().expect("Invalid input");
                snap
            }
        };
        // end_snapshot has to exists as row.number, otherwise error out.
        snapshots.iter()
            .find(|&row| row.number == end_snapshot)
            .with_context(|| format!("Unable to find end snapshot number: {}", end_snapshot))?;
        // all information is found and collected, return it to the caller.
        Ok((begin_snapshot.to_string(), end_snapshot.to_string(), begin_snapshot_row.clone()))
    }
}

pub fn save_snapshot<T: Serialize>(
    snapshot_number: i32,
    filename: &str,
    vector: Vec<T>,
) -> Result<()>
{
    let current_directory = env::current_dir()?;
    let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(&snapshot_number.to_string());

    let filepath = &current_snapshot_directory.join(filename);
    let file = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(filepath)
        .with_context(|| format!("Error saving snapshot: {}", filepath.display()))?;
    let mut writer = csv::Writer::from_writer(file);
    for row in vector {
        writer.serialize(row)?;
    }
    writer.flush()?;

    Ok(())
}

pub fn read_snapshot<T: for<'de> Deserialize<'de>>(
    snapshot_number: &String,
    filename: &str,
) -> Result<Vec<T>>
{
    let mut vector = Vec::new();

    let current_directory = env::current_dir()?;
    let current_snapshot_directory = current_directory.join("yb_stats.snapshots").join(snapshot_number);

    let filepath = &current_snapshot_directory.join(filename);
    let file = fs::File::open(filepath)
        .with_context(|| format!("Error reading snapshot: {}", filepath.display()))?;

    let mut reader = csv::Reader::from_reader(file);
    for row in reader.deserialize() {
        let data: T = row?;
        vector.push(data);
    };

    Ok(vector)
}