use chrono;

pub mod influx;
pub mod print;

use self::influx::{InfluxSink, InfluxConfig};
use self::print::{PrintSink, PrintConfig};

use self::chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt;

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum SinkConfig {
    Influx(InfluxConfig),
    Print(PrintConfig),
}

pub trait Sink {
    fn add_measurement(&mut self, _: &Measurement);
    fn submit(&mut self);
}

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::String(val) => write!(f, "{}", val),
            Value::Integer(val) => write!(f, "{}", val),
            Value::Float(val) => write!(f, "{:0.4}", val),
        }
    }
}

pub fn from_config(sink_configs: &Vec<SinkConfig>) -> Vec<Box<dyn Sink>> {
    let mut sinks = Vec::new();
    for sink_config in sink_configs {
        sinks.push(match sink_config {
            SinkConfig::Influx(val) => InfluxSink::from_config(val),
            SinkConfig::Print(val) => PrintSink::from_config(val),
        });
    }
    sinks
}

#[derive(Debug, Clone)]
pub struct Measurement {
    pub measurement: String,
    pub fields: HashMap<String, Value>,
    pub tags: HashMap<String, Value>,
    pub timestamp: DateTime<Utc>,
}

impl Measurement {
    pub fn new(measurement: &str) -> Measurement {
        Measurement {
            measurement: String::from(measurement),
            fields: HashMap::new(),
            tags: HashMap::new(),
            timestamp: Utc::now(),
        }
    }

    pub fn field(mut self, name: &str, value: Value) -> Self {
        self.fields.insert(String::from(name), value);
        self
    }

    pub fn tag(mut self, name: &str, value: Value) -> Self {
        self.tags.insert(String::from(name), value);
        self
    }
}

impl fmt::Display for Measurement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} ",
            self.measurement,
            self.timestamp.format("%Y-%m-%d %H:%M:%S%.f")
        )?;

        let mut tags: Vec<_> = self.tags.iter().collect();
        tags.sort_by(|a, b| a.0.cmp(b.0));
        for (idx, (key, value)) in tags.iter().enumerate() {
            if idx != 0 {
                write!(f, ",")?;
            }
            write!(f, "{}={}", key, value)?;
        }
        if tags.len() > 0 {
            write!(f, " ")?;
        }

        let mut fields: Vec<_> = self.fields.iter().collect();
        fields.sort_by(|a, b| a.0.cmp(b.0));
        for (idx, (key, value)) in fields.iter().enumerate() {
            if idx != 0 {
                write!(f, ",")?;
            }
            write!(f, "{}={}", key, value)?;
        }
        Ok(())
    }
}
