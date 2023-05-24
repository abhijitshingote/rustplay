// cargo run -- s3://smrf-mrm-test/barry-test/prov_intel_test3/57/202302/rep_plan_group_id_534.parquet/npi=1003000399
// s3://smrf-mrm-test/abi-test/prov_intel_mover/57/202302/npi=1003000399

// cargo run -- s3://smrf-mrm/alliedbenefits/prepped/202305/2022-12-26-innrfppog12022022-cms_in_network_rates_ppo_2022-12-02_AK.json/in_network.jsonl 100

use aws_sdk_s3 as s3;
use aws_sdk_s3::Client;
use clap::Parser;
use tokio::io::{AsyncBufReadExt, BufReader};
use std::{process};
use serde::{Serialize,Deserialize};
use serde_json;
use std::time::Instant;


#[tokio::main]
async fn main() -> Result<(), s3::Error> {
    let before = Instant::now();
    let args = Opt::parse();
    let config = aws_config::load_from_env().await;
    let client = s3::Client::new(&config);
    // // copy(&client, source_path, dest_path).await;
    let path = parse_s3_uri(&args.file_name);
    println!("{:?}", path);
    read_s3_object(&path, &client, NumLines::Num(args.num_lines)).await;
    println!("Took {:2?} to complete",before.elapsed());
    Ok(())
}
#[derive(Default, Parser, Debug)]
struct Opt {
    file_name: String,
    num_lines: i32,
}

#[derive(Serialize,Deserialize,Debug)]
struct Rate {
    billing_code: String,
    billing_code_type: String,
    negotiation_arrangement: String,
    negotiated_rates: Vec<NegRate>
}
#[derive(Serialize,Deserialize,Debug)]
struct NegPrice {
    negotiated_type: String,
    negotiated_rate: f32,
    expiration_date: String,
    service_code: Vec<String>,
    billing_class: String
}
#[derive(Serialize,Deserialize,Debug)]
struct NegRate {
    negotiated_prices: Vec<NegPrice>,
    provider_references: Vec<usize>
}

enum NumLines {
    Everything,
    Num(i32),
}

async fn read_s3_object(s3_path: &S3Path, client: &Client, n: NumLines) {
    let obj = client
        .get_object()
        .bucket(&s3_path.bucket)
        .key(&s3_path.key)
        .send()
        .await
        .expect("Issues get object!");

    match n {
        NumLines::Everything => {
            let objbodyobj = obj.body.collect().await.expect("issues reading!");
            println!("{:?}", objbodyobj);
        }
        NumLines::Num(num) => {
            let buf_reader = BufReader::new(obj.body.into_async_read());
            let mut lines = buf_reader.lines();
            // let l =lines.next_line()
            for _ in 0..num {
                let line = lines
                    .next_line()
                    .await
                    .unwrap_or_else(|err| {process::exit(0)})
                    .unwrap_or_else(|| {
                        println!("Maybe end of file!");
                        process::exit(0);});
                // let snip= line.find(r#""billing_code""#).unwrap();
                // println!("{}", &line[snip..snip+22]);
                // println!("{}",&line);
                let r: Rate = serde_json::from_str(&line).unwrap();
                // println!("{:?},{:?}",r.billing_code,r.negotiated_rates[0].negotiated_prices[0].negotiated_rate);
                let mut num_neg_prices = 0;
                let mut num_prov_ref = 0;
                for nr in &r.negotiated_rates {
                    num_neg_prices += nr.negotiated_prices.len();
                    num_prov_ref += nr.provider_references.len();
                }
                println!("Billing Code:{:?}, {:?} # of Neg Rate Obj N {:?},TOTAL Neg Prices Q {:?}, TOTAL Provider References P {:?}",
                r.billing_code_type,r.billing_code,r.negotiated_rates.len(),&num_neg_prices,&num_prov_ref);
            }
        }
    }
}

async fn copy(client: &Client, source_path: S3Path, dest_path: S3Path) {
    let source_paths = list_objects(&source_path, &client).await.unwrap();
    println!("Objects to copy ......{:?}", source_paths);
    for sp in source_paths {
        let mut sp_w_bucket = String::new();
        sp_w_bucket.push_str(&source_path.bucket);
        sp_w_bucket.push_str("/");
        sp_w_bucket.push_str(&sp);
        let mut dest_key = String::from(&dest_path.key);
        let dest_vec: Vec<&str> = sp.split("/").collect();
        dest_key.push_str(dest_vec[dest_vec.len() - 1]);
        let cp = client
            .copy_object()
            .copy_source(&sp_w_bucket)
            .bucket(&dest_path.bucket)
            .key(&dest_key)
            .send()
            .await
            .unwrap();
        println!("{:?}", cp);
    }
}

async fn list_objects(s3_path: &S3Path, client: &Client) -> Result<Vec<String>, aws_sdk_s3::Error> {
    let sourcelist = client
        .list_objects()
        .bucket(&s3_path.bucket)
        .prefix(&s3_path.key)
        .send()
        .await?;
    let mut paths: Vec<String> = Vec::new();
    for o in sourcelist.contents().unwrap() {
        let key = o.key().unwrap();
        paths.push(key.to_owned());
    }
    Ok(paths)
}

fn parse_s3_uri(path: &str) -> S3Path {
    let v: Vec<&str> = path.split('/').collect();
    let bucket = v[2].to_owned();
    let key = v[3..].join("/");
    S3Path { bucket, key }
}

#[derive(Debug)]
struct S3Path {
    bucket: String,
    key: String,
}
