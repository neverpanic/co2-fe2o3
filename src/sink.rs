extern crate chrono;

pub mod influx;

use self::influx::{InfluxSink, InfluxConfig};

use self::chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::fmt;

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum SinkConfig {
    Influx(InfluxConfig),
}

pub trait Sink {
    fn add_measurement(&mut self, &Measurement);
    fn submit(&mut self);
}

#[derive(Debug, Clone)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Value::String(val) => write!(f, "{}", val),
            Value::Integer(val) => write!(f, "{}", val),
            Value::Float(val) => write!(f, "{:0.4}", val),
        }
    }
}

pub fn from_config(sink_configs: &Vec<SinkConfig>) -> Vec<Box<Sink>> {
    let mut sinks = Vec::new();
    for sink_config in sink_configs {
        sinks.push(match sink_config {
            SinkConfig::Influx(val) => InfluxSink::from_config(val),
        });
    }
    sinks
}

#[derive(Debug, Clone)]
pub struct Measurement<'a> {
    pub measurement: &'a str,
    pub fields: HashMap<&'a str, Value>,
    pub tags: HashMap<&'a str, Value>,
    pub timestamp: DateTime<Utc>,
}

impl<'a> Measurement<'a> {
    pub fn new(measurement: &'a str) -> Measurement {
        let fields = HashMap::new();
        let tags = HashMap::new();
        let timestamp = Utc::now();
        Measurement{measurement, fields, tags, timestamp}
    }

    pub fn field(&mut self, name: &'a str, value: Value) -> &mut Self {
        self.fields.insert(name, value);
        self
    }

    pub fn tag(&mut self, name: &'a str, value: Value) -> &mut Self {
        self.tags.insert(name, value);
        self
    }
}

impl<'a> fmt::Display for Measurement<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} {} ", self.measurement, self.timestamp.format("%Y-%m-%d %H:%M:%S%.f"))?;

        let mut tags : Vec<_> = self.tags.iter().collect();
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

        let mut fields : Vec<_> = self.fields.iter().collect();
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
