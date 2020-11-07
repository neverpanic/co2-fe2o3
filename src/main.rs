#[macro_use]
extern crate serde_derive;
use futures::join;
use tokio::sync::mpsc::{channel, Sender, Receiver, error::TryRecvError};
use tokio::time::{interval, Duration};
use toml;

mod sensor;
mod sink;

use crate::sensor::Sensor;
use crate::sink::SinkConfig;

use std::env;
use std::error;
use std::fs;
use std::process;

#[derive(Deserialize)]
struct Config {
    poll_interval: Option<u64>,
    sink: Vec<SinkConfig>,
}

fn parse_config(filename: &str) -> Result<Config, Box<dyn error::Error>> {
    let config: Config = toml::from_str(&fs::read_to_string(filename)?)?;
    Ok(config)
}

async fn consumer(config_sinks: &Vec<SinkConfig>, mut rx: Receiver<sink::Measurement>) {
    let mut sinks = sink::from_config(&config_sinks);
    let mut interval = interval(Duration::from_secs(1));

    loop {
        interval.tick().await;

        for sink in &mut sinks {
            sink.submit().await;
        }

        match rx.try_recv() {
            Ok(measurement) => {
                for sink in &mut sinks {
                    sink.add_measurement(&measurement).await;
                }
            }
            Err(TryRecvError::Closed) => break,
            Err(TryRecvError::Empty) => continue,
        }
    }
    for sink in &mut sinks {
        sink.submit().await;
    }
}

async fn producer(config_poll_interval: Option<u64>, mut tx: Sender<sink::Measurement>) {
    let mut devices = match Sensor::sensors() {
        None => vec![],
        Some(devices) => devices,
    };

    let poll_interval = match config_poll_interval {
        Some(i) => Duration::from_secs(i),
        None => Duration::from_millis(1),
    };

    let mut interval = interval(poll_interval);

    while devices.len() > 0 {
        interval.tick().await;

        let mut measurements = vec![];
        devices.retain(|device| match device.read() {
            None => false,
            Some(measurement) => {
                measurements.push(measurement);
                true
            }
        });

        for measurement in measurements {
            tx.send(measurement).await.unwrap();
        }
    }

    drop(tx);
}

#[tokio::main()]
async fn main() {
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

    let (tx, rx) = channel::<sink::Measurement>(50);

    let poll_interval = config.poll_interval;
    let consumer = tokio::spawn(async move {
        consumer(&config.sink, rx).await;
    });

    let producer = tokio::spawn(async move {
        producer(poll_interval, tx).await;
        eprintln!("No devices left to query, exiting after all data has been written!");
    });

    let (producer_result, consumer_result) = join!(producer, consumer);
    producer_result.unwrap();
    consumer_result.unwrap();

    process::exit(1);
}
