// cargo run -- s3://smrf-mrm-test/barry-test/prov_intel_test3/57/202302/rep_plan_group_id_534.parquet/npi=1003000399
// s3://smrf-mrm-test/abi-test/prov_intel_mover/57/202302/npi=1003000399

use aws_sdk_s3 as s3;
use aws_sdk_s3::Client;
use tokio::io::{AsyncBufReadExt, BufReader};

#[tokio::main]
async fn main() -> Result<(), s3::Error> {
    let args: Vec<String> = std::env::args().collect();
    let source_path = parse_s3_uri(&args[1]);
    let dest_path = parse_s3_uri(&args[2]);
    println!(
        "Source Path - {:?}\nDestination - {:?}",
        source_path, dest_path
    );
    let config = aws_config::load_from_env().await;
    let client = s3::Client::new(&config);
    copy(&client, source_path, dest_path).await;
    let s3uri="s3://smrf-mrm-test/abi-test/aetnamrf-27440.csv/part-00000-b6babe28-9b16-4ab4-a36a-918926ae82fd-c000.csv";
    let path = parse_s3_uri(s3uri);
    println!("{:?}", path);
    read_s3_object(&path, &client, NumLines::Everything).await;
    Ok(())
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
            for _ in 0..5 {
                println!(
                    "Printing data {:?}",
                    lines
                        .next_line()
                        .await
                        .expect("failed reading line")
                        .unwrap()
                );
                println!(
                    "Printing data {:?}",
                    lines
                        .next_line()
                        .await
                        .expect("failed reading line")
                        .unwrap()
                );
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
