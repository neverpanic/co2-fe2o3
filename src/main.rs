extern crate co2_fe2o3;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate toml;

mod sink;

use co2_fe2o3::Sensor;
use sink::{Measurement, Value, SinkConfig};

use std::env;
use std::error;
use std::fs;
use std::process;
use std::sync::mpsc::{sync_channel, TryRecvError};
use std::thread;
use std::time::Duration;

#[derive(Deserialize)]
struct Config {
    sink: Vec<SinkConfig>,
}

fn parse_config(filename: &str) -> Result<Config, Box<error::Error>> {
    let config: Config = toml::from_str(&fs::read_to_string(filename)?)?;
    Ok(config)
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        println!("Usage: {} config-file", args[0]);
        process::exit(1);
    }

    let config = match parse_config(&args[1]) {
        Ok(config) => config,
        Err(err) => {
            println!("Failed to parse configuration file {}: {}", args[1], err);
            process::exit(1);
        }
    };

    let mut devices = match Sensor::sensors() {
        None => vec![],
        Some(devices) => devices
    };

    let (tx, rx) = sync_channel::<sink::Measurement>(10);
    let writer = thread::spawn(move|| {
        let mut sinks = sink::from_config(&config.sink);

        loop {
            thread::sleep(Duration::from_secs(1));
            for sink in &mut sinks {
                sink.submit()
            }
            match rx.try_recv() {
                Ok(measurement) =>
                    for sink in &mut sinks {
                        sink.add_measurement(&measurement)
                    },
                Err(TryRecvError::Disconnected) => break,
                Err(TryRecvError::Empty) => continue,
            }
        }
        for sink in &mut sinks {
            sink.submit()
        }
    });

    while devices.len() > 0 {
        devices = devices.into_iter().filter_map(|device|
            match device.read() {
                None => return None,
                Some(value) => {
                    let mut measurement = Measurement::new("room_climate");
                    measurement
                        .tag("sensor", Value::String(device.name().to_string()))
                        .field("co2", Value::Integer(value.co2ppm as i64))
                        .field("temperature", Value::Float(value.temperature));
                    println!("{}", measurement);
                    tx.send(measurement).unwrap();
                    Some(device)
                }
            }).collect();
    }

    eprintln!("No devices left to query, exiting after all data has been written!");

    drop(tx);
    writer.join().unwrap();

    process::exit(1);
}
