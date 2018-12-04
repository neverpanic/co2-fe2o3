extern crate co2_fe2o3;

use co2_fe2o3::Sensor;

use std::process;

use std::time::{SystemTime, UNIX_EPOCH};

fn format_timestamp(timestamp: SystemTime) -> u128 {
    let ts = match timestamp.duration_since(UNIX_EPOCH) {
        Ok(time) => time,
        Err(_) => return 0
    };
    (ts.as_secs() as u128) * 1_000_000_000 + (ts.subsec_nanos() as u128)
}

fn main() {
    let mut devices = match Sensor::sensors() {
        None => vec![],
        Some(devices) => devices
    };

    while devices.len() > 0 {
        devices = devices.into_iter().filter_map(|device|
            match device.read() {
                None => return None,
                Some(value) => {
                    println!("room_climate,sensor={} co2={}i,temperature={} {}",
                             device.name(),
                             value.co2ppm,
                             value.temperature,
                             format_timestamp(value.timestamp));
                    Some(device)
                }
            }).collect();
    }

    eprintln!("No devices left to query, exiting!");
    process::exit(1);
}
