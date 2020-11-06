use chrono;
use influx_db_client;
use async_trait::async_trait;

use self::chrono::{Utc, Duration};
use self::influx_db_client::{Client, Point, Points, Value, Precision, point};
use super::{Sink, Value as SinkValue};


#[derive(Deserialize)]
pub struct InfluxConfig {
    pub host: String,
    pub database: String,
    pub user: String,
    pub pass: String,
    pub bulk_time: i64,
}

pub struct InfluxSink {
    client: Client,
    bulk_time: Duration,
    points: Points,
}

trait ToInflux {
    fn to_influx(&self) -> Value;
}

impl ToInflux for SinkValue {
    fn to_influx(&self) -> Value {
        match self {
            SinkValue::String(val) => Value::String(val.to_owned()),
            SinkValue::Integer(val) => Value::Integer(*val),
            SinkValue::Float(val) => Value::Float(*val),
        }
    }
}

#[async_trait]
impl Sink for InfluxSink {
    fn add_measurement(&mut self, measurement: &super::Measurement) {
        let mut point = point!(&measurement.measurement);
        for (key, value) in &measurement.fields {
            point = point.add_field(key, value.to_influx());
        }
        for (key, value) in &measurement.tags {
            point = point.add_tag(key, value.to_influx());
        }
        point = point.add_timestamp(measurement.timestamp.timestamp_nanos());

        self.points.point.push(point.to_owned());
    }

    async fn submit(&mut self) -> () {
        let mut submit = false;
        if self.points.point.len() > 0 {
            submit = match self.points.point[0].timestamp {
                None => false,
                Some(ts) => {
                    let expiry_boundary = ts + self.bulk_time.num_nanoseconds().unwrap_or(0);
                    let now = Utc::now().timestamp_nanos();
                    expiry_boundary < now
                }
            }
        }
        if submit {
            let num_points = self.points.point.len();
            match self.client.write_points(
                &mut self.points,
                Some(Precision::Nanoseconds),
                None,
            ).await {
                Ok(_) => {
                    println!("----- {} submitted -----", num_points);
                    self.points = Points::create_new(Vec::new())
                }
                Err(err) => println!("Failed to submit points: {}", err),
            }
        }
    }
}

impl InfluxSink {
    pub fn from_config(config: &InfluxConfig) -> Box<dyn Sink> {
        let client = Client::new(
            config.host.parse().unwrap(),
            config.database.to_owned()
        ).set_authentication(config.user.to_owned(), config.pass.to_owned());
        let bulk_time = Duration::seconds(config.bulk_time);
        let points = Points::create_new(Vec::new());
        Box::new(InfluxSink {
            client,
            bulk_time,
            points,
        })
    }
}
