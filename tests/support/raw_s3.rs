// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Minimal path-style S3 client used to preserve raw key bytes in acceptance.

use hmac::{Hmac, KeyInit, Mac};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RawObject {
    pub bytes: Vec<u8>,
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub version: Option<String>,
}

pub type RawInventory = BTreeMap<String, RawObject>;

#[derive(Clone)]
pub struct RawS3 {
    bucket: String,
    host: String,
    region: String,
    access_key: String,
    secret_key: String,
}

impl RawS3 {
    pub fn from_env() -> Self {
        let endpoint = required("YINYANG_S3_ENDPOINT");
        let host = endpoint
            .strip_prefix("http://")
            .unwrap_or_else(|| {
                panic!(
                    "Direct raw-key acceptance uses the isolated plain-HTTP Silo endpoint; got {endpoint}"
                )
            })
            .trim_end_matches('/')
            .to_owned();
        assert!(
            !host.contains('/'),
            "Direct acceptance endpoint must not contain a path: {endpoint}"
        );
        Self {
            bucket: required("YINYANG_S3_BUCKET"),
            host,
            region: required("YINYANG_S3_REGION"),
            access_key: required("YINYANG_S3_ACCESS_KEY_ID"),
            secret_key: required("YINYANG_S3_SECRET_ACCESS_KEY"),
        }
    }

    #[allow(dead_code)]
    pub fn with_bucket(&self, bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            ..self.clone()
        }
    }

    pub fn put(&self, key: &str, bytes: &[u8]) {
        let response = self.request("PUT", Some(key), "", bytes);
        assert!(
            (200..300).contains(&response.status),
            "raw S3 PUT {key:?} failed with {}:\n{}",
            response.status,
            String::from_utf8_lossy(&response.body)
        );
    }

    pub fn delete(&self, key: &str) {
        let response = self.request("DELETE", Some(key), "", &[]);
        assert!(
            (200..300).contains(&response.status),
            "raw S3 DELETE {key:?} failed with {}:\n{}",
            response.status,
            String::from_utf8_lossy(&response.body)
        );
    }

    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        let response = self.request("GET", Some(key), "", &[]);
        match response.status {
            200 => Some(response.body),
            404 => None,
            status => panic!(
                "raw S3 GET {key:?} failed with {status}:\n{}",
                String::from_utf8_lossy(&response.body)
            ),
        }
    }

    #[allow(dead_code)]
    pub fn get_version(&self, key: &str, version: &str) -> Option<Vec<u8>> {
        let query = format!("versionId={}", query_encode(version));
        let response = self.request("GET", Some(key), &query, &[]);
        match response.status {
            200 => Some(response.body),
            404 => None,
            status => panic!(
                "raw S3 GET {key:?} version {version} failed with {status}:\n{}",
                String::from_utf8_lossy(&response.body)
            ),
        }
    }

    pub fn inventory(&self, prefix: &str) -> RawInventory {
        let mut keys = Vec::new();
        let mut token: Option<String> = None;
        loop {
            let mut query = vec![
                ("encoding-type".to_owned(), "url".to_owned()),
                ("list-type".to_owned(), "2".to_owned()),
                ("prefix".to_owned(), prefix.to_owned()),
            ];
            if let Some(token) = &token {
                query.push(("continuation-token".to_owned(), token.clone()));
            }
            query.sort();
            let query = query
                .into_iter()
                .map(|(key, value)| format!("{}={}", query_encode(&key), query_encode(&value)))
                .collect::<Vec<_>>()
                .join("&");
            let response = self.request("GET", None, &query, &[]);
            assert_eq!(
                response.status,
                200,
                "raw S3 LIST failed:\n{}",
                String::from_utf8_lossy(&response.body)
            );
            let body = String::from_utf8(response.body).expect("S3 ListObjectsV2 is UTF-8 XML");
            for encoded in xml_values(&body, "Key") {
                let key = utf8(percent_decode(&xml_unescape(&encoded)), "listed S3 key");
                keys.push(key);
            }
            token = xml_values(&body, "NextContinuationToken")
                .into_iter()
                .next()
                .map(|token| xml_unescape(&token));
            if token.is_none() {
                break;
            }
        }
        keys.sort();
        keys.into_iter()
            .map(|key| {
                let bytes = self
                    .get(&key)
                    .unwrap_or_else(|| panic!("listed raw S3 key {key:?} disappeared"));
                let head = self
                    .head(&key)
                    .unwrap_or_else(|| panic!("listed raw S3 key {key:?} has no HEAD metadata"));
                (
                    key,
                    RawObject {
                        bytes,
                        etag: head.etag,
                        last_modified: head.last_modified,
                        version: head.version,
                    },
                )
            })
            .collect()
    }

    fn head(&self, key: &str) -> Option<Head> {
        let response = self.request("HEAD", Some(key), "", &[]);
        if response.status == 404 {
            return None;
        }
        assert_eq!(
            response.status,
            200,
            "raw S3 HEAD {key:?} failed:\n{}",
            String::from_utf8_lossy(&response.body)
        );
        Some(Head {
            etag: response.header("etag").map(str::to_owned),
            last_modified: response.header("last-modified").map(str::to_owned),
            version: response.header("x-amz-version-id").map(str::to_owned),
        })
    }

    fn request(&self, method: &str, key: Option<&str>, query: &str, body: &[u8]) -> Response {
        let canonical_uri = match key {
            Some(key) => format!(
                "/{}/{}",
                path_encode(&self.bucket),
                path_encode_with_slashes(key)
            ),
            None => format!("/{}", path_encode(&self.bucket)),
        };
        let now = HttpDateTime::now();
        let payload_hash = hex(&Sha256::digest(body));
        let canonical_headers = format!(
            "host:{}\nx-amz-content-sha256:{}\nx-amz-date:{}\n",
            self.host, payload_hash, now.amz
        );
        let signed_headers = "host;x-amz-content-sha256;x-amz-date";
        let canonical_request = format!(
            "{method}\n{canonical_uri}\n{query}\n{canonical_headers}\n{signed_headers}\n\
             {payload_hash}"
        );
        let scope = format!("{}/{}/s3/aws4_request", now.date, self.region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            now.amz,
            scope,
            hex(&Sha256::digest(canonical_request.as_bytes()))
        );
        let date_key = hmac(
            format!("AWS4{}", self.secret_key).as_bytes(),
            now.date.as_bytes(),
        );
        let region_key = hmac(&date_key, self.region.as_bytes());
        let service_key = hmac(&region_key, b"s3");
        let signing_key = hmac(&service_key, b"aws4_request");
        let signature = hex(&hmac(&signing_key, string_to_sign.as_bytes()));
        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.access_key, scope, signed_headers, signature
        );
        let mut request = format!(
            "{method} {canonical_uri}{} HTTP/1.1\r\n\
             Host: {}\r\n\
             x-amz-content-sha256: {payload_hash}\r\n\
             x-amz-date: {}\r\n\
             Authorization: {authorization}\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n\r\n",
            if query.is_empty() {
                String::new()
            } else {
                format!("?{query}")
            },
            self.host,
            now.amz,
            body.len()
        )
        .into_bytes();
        request.extend_from_slice(body);

        let mut stream = TcpStream::connect(&self.host)
            .unwrap_or_else(|error| panic!("connect to raw S3 endpoint {}: {error}", self.host));
        stream
            .set_read_timeout(Some(Duration::from_secs(60)))
            .expect("set raw S3 read timeout");
        stream
            .set_write_timeout(Some(Duration::from_secs(60)))
            .expect("set raw S3 write timeout");
        stream
            .write_all(&request)
            .unwrap_or_else(|error| panic!("write raw S3 request: {error}"));
        let mut bytes = Vec::new();
        stream
            .read_to_end(&mut bytes)
            .unwrap_or_else(|error| panic!("read raw S3 response: {error}"));
        response(bytes)
    }
}

struct Head {
    etag: Option<String>,
    last_modified: Option<String>,
    version: Option<String>,
}

struct Response {
    status: u16,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}

impl Response {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers.get(name).map(String::as_str)
    }
}

fn response(mut bytes: Vec<u8>) -> Response {
    let boundary = bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .expect("raw S3 response has a header terminator");
    let body = bytes.split_off(boundary + 4);
    let head = String::from_utf8(bytes).expect("S3 response headers are UTF-8");
    let mut lines = head.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|status| status.parse::<u16>().ok())
        .expect("raw S3 status line");
    let mut headers = BTreeMap::new();
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_owned());
    }
    let body = if headers
        .get("transfer-encoding")
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        chunked(body)
    } else {
        body
    };
    Response {
        status,
        headers,
        body,
    }
}

fn chunked(mut bytes: Vec<u8>) -> Vec<u8> {
    let mut output = Vec::new();
    loop {
        let boundary = bytes
            .windows(2)
            .position(|window| window == b"\r\n")
            .expect("chunk size line");
        let line =
            String::from_utf8(bytes.drain(..boundary + 2).collect()).expect("chunk size is ASCII");
        let size = usize::from_str_radix(line.trim(), 16).expect("chunk size");
        if size == 0 {
            return output;
        }
        output.extend(bytes.drain(..size));
        assert_eq!(
            bytes.drain(..2).collect::<Vec<_>>(),
            b"\r\n",
            "chunk terminator"
        );
    }
}

fn hmac(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn required(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| panic!("{name} is required for Direct acceptance"))
}

fn path_encode(value: &str) -> String {
    encode(value.as_bytes(), false)
}

fn path_encode_with_slashes(value: &str) -> String {
    encode(value.as_bytes(), true)
}

fn query_encode(value: &str) -> String {
    encode(value.as_bytes(), false)
}

fn encode(bytes: &[u8], keep_slash: bool) -> String {
    let mut output = String::new();
    for &byte in bytes {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            output.push(char::from(byte));
        } else if keep_slash && byte == b'/' {
            output.push('/');
        } else {
            output.push_str(&format!("%{byte:02X}"));
        }
    }
    output
}

fn percent_decode(value: &str) -> Vec<u8> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            assert!(
                index + 2 < bytes.len(),
                "S3 URL encoding has a truncated percent escape"
            );
            output.push((nibble(bytes[index + 1]) << 4) | nibble(bytes[index + 2]));
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    output
}

fn nibble(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        b'A'..=b'F' => value - b'A' + 10,
        _ => panic!("invalid S3 percent escape"),
    }
}

fn utf8(bytes: Vec<u8>, context: &str) -> String {
    String::from_utf8(bytes).unwrap_or_else(|error| panic!("{context} is not UTF-8: {error}"))
}

fn xml_values(body: &str, tag: &str) -> Vec<String> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let mut values = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find(&open) {
        rest = &rest[start + open.len()..];
        let end = rest
            .find(&close)
            .unwrap_or_else(|| panic!("unterminated {tag} XML element"));
        values.push(rest[..end].to_owned());
        rest = &rest[end + close.len()..];
    }
    values
}

fn xml_unescape(value: &str) -> String {
    value
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}

struct HttpDateTime {
    date: String,
    amz: String,
}

impl HttpDateTime {
    fn now() -> Self {
        let seconds = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time after Unix epoch")
            .as_secs();
        let days = (seconds / 86_400) as i64;
        let day_seconds = seconds % 86_400;
        let (year, month, day) = civil_from_days(days);
        let hour = day_seconds / 3_600;
        let minute = (day_seconds % 3_600) / 60;
        let second = day_seconds % 60;
        HttpDateTime {
            date: format!("{year:04}{month:02}{day:02}"),
            amz: format!("{year:04}{month:02}{day:02}T{hour:02}{minute:02}{second:02}Z"),
        }
    }
}

fn civil_from_days(days_since_epoch: i64) -> (i64, i64, i64) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let day_of_era = z - era * 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = month_prime + if month_prime < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    (year, month, day)
}
