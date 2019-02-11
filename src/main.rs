extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate toml;

mod sensor;
mod sink;

use sensor::Sensor;
use sink::SinkConfig;

use std::env;
use std::error;
use std::fs;
use std::process;
use std::sync::mpsc::{channel, TryRecvError};
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

    let (tx, rx) = channel::<sink::Measurement>();
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

    let mut devices = match Sensor::sensors() {
        None => vec![],
        Some(devices) => devices
    };

    while devices.len() > 0 {
        devices.retain(|device|
            match device.read() {
                None => false,
                Some(measurement) => {
                    println!("{}", measurement);
                    tx.send(measurement).unwrap();
                    true
                }
            }
        );
    }

    eprintln!("No devices left to query, exiting after all data has been written!");

    drop(tx);
    writer.join().unwrap();

    process::exit(1);
}
