use hidapi;
use rand;
use crypto;


use self::crypto::digest::Digest;
use self::crypto::sha2::Sha256;
use self::hidapi::{DeviceInfo, HidDevice, HidApi};
use self::rand::Rng;

use super::sink::{Measurement, Value};

const CO2_SENSOR_VENDOR: u16 = 0x4d9;
const CO2_SENSOR_PRODUCT: u16 = 0xa052;

const METER_CO2: u8 = 0x50;
const METER_TEMP: u8 = 0x42;

pub struct Sensor {
    name: String,
    device: HidDevice,
    key: [u8; 8],
}

fn sha256(input: &[u8]) -> String {
    let mut sha256 = Sha256::new();
    sha256.input(input);
    sha256.result_str()
}

impl Sensor {
    fn new(api: &HidApi, info: &DeviceInfo) -> Option<Sensor> {
        // Open device
        let device = match info.open_device(api) {
            Ok(device) => device,
            Err(_) => return None,
        };

        // Generate key
        let mut key: [u8; 8] = [0; 8];
        rand::thread_rng().fill(&mut key[..]);

        // Send key to device
        let mut buf: [u8; 9] = [0; 9];
        &buf[1..9].clone_from_slice(&key[..]);
        if device.send_feature_report(&buf).is_err() {
            return None;
        }

        // Generate (somewhat) human-readable name
        let name = sha256(info.path().to_bytes());
        Some(Sensor { name, device, key })
    }

    pub fn sensors() -> Option<Vec<Sensor>> {
        let api = match HidApi::new() {
            Ok(api) => api,
            Err(_) => return None,
        };
        let sensors = api.device_list()
            .into_iter()
            .filter(|info| info.vendor_id() == CO2_SENSOR_VENDOR)
            .filter(|info| info.product_id() == CO2_SENSOR_PRODUCT)
            .filter_map(|info| Sensor::new(&api, info))
            .collect();
        Some(sensors)
    }

    pub fn read(&self) -> Option<Measurement> {
        let mut temperature: Option<f64> = None;
        let mut co2ppm: Option<u32> = None;
        let mut counter = 0;

        loop {
            let mut buf: [u8; 8] = [0; 8];
            let size = match self.device.read(&mut buf) {
                Ok(size) => size,
                Err(_) => return None,
            };
            if size != buf.len() {
                return None;
            }

            let decrypted = self.decrypt(&buf);
            let value = (decrypted[1] as u16) << 8 | (decrypted[2] as u16);
            match decrypted[0] {
                METER_CO2 => co2ppm = Some(value as u32),
                METER_TEMP => {
                    temperature = Some((((value as f64) / 16.0 - 273.15) * 10.0).round() / 10.0)
                }
                _ => continue,
            }

            counter += 1;
            if counter > 5 {
                return None;
            }

            let temp = match temperature {
                Some(val) => val,
                None => continue,
            };
            let co2 = match co2ppm {
                Some(val) => val,
                None => continue,
            };
            return Some(
                Measurement::new("room_climate")
                    .tag("sensor", Value::String(self.name().to_string()))
                    .field("co2", Value::Integer(co2 as i64))
                    .field("temperature", Value::Float(temp)),
            );
        }
    }

    fn decrypt(&self, buf: &[u8; 8]) -> [u8; 8] {
        let state: [u8; 8] = [0x84, 0x47, 0x56, 0xd6, 0x07, 0x93, 0x93, 0x56];
        let shuffle: [usize; 8] = [2, 4, 0, 7, 1, 6, 5, 3];

        let data = buf;
        let mut accumulator: [u8; 8] = [0; 8];
        for (src, dst) in shuffle.iter().enumerate() {
            accumulator[*dst] = data[src];
        }

        let data = accumulator;
        for (idx, k) in self.key.iter().enumerate() {
            accumulator[idx] = data[idx] ^ k;
        }

        let data = accumulator;
        for idx in 0..8 {
            accumulator[idx] = ((data[idx] >> 3) | (data[(idx + 8 - 1) % 8] << 5)) & 0xff;
        }

        let data = accumulator;
        for idx in 0..8 {
            accumulator[idx] = ((0x100 + (data[idx] as u16) - (state[idx] as u16)) & 0xff) as u8;
        }

        accumulator
    }

    pub fn name(&self) -> &str {
        &self.name
    }
}
