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
use std::time::{Duration, Instant};

#[derive(Deserialize)]
struct Config {
    poll_interval: Option<u64>,
    sink: Vec<SinkConfig>,
}

fn parse_config(filename: &str) -> Result<Config, Box<dyn error::Error>> {
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

    let config_sinks = config.sink;
    let (tx, rx) = channel::<sink::Measurement>();
    let writer = thread::spawn(move || {
        let mut sinks = sink::from_config(&config_sinks);

        loop {
            thread::sleep(Duration::from_secs(1));
            for sink in &mut sinks {
                sink.submit()
            }
            match rx.try_recv() {
                Ok(measurement) => {
                    for sink in &mut sinks {
                        sink.add_measurement(&measurement)
                    }
                }
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
        Some(devices) => devices,
    };

    let poll_interval = match config.poll_interval {
        Some(i) => Duration::from_secs(i),
        None => Default::default(),
    };

    while devices.len() > 0 {
        let begin_poll = Instant::now();

        devices.retain(|device| match device.read() {
            None => false,
            Some(measurement) => {
                tx.send(measurement).unwrap();
                true
            }
        });

        let elapsed = Instant::now() - begin_poll;
        let sleepfor = poll_interval
            .checked_sub(elapsed)
            .unwrap_or_default();
        thread::sleep(sleepfor);
    }

    eprintln!("No devices left to query, exiting after all data has been written!");

    drop(tx);
    writer.join().unwrap();

    process::exit(1);
}
