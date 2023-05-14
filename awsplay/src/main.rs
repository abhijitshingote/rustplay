// cargo run -- s3://smrf-mrm-test/barry-test/prov_intel_test3/57/202302/rep_plan_group_id_534.parquet/npi=1003000399 
// s3://smrf-mrm-test/abi-test/prov_intel_mover/57/202302/npi=1003000399

use aws_sdk_s3 as s3;
use aws_sdk_s3::Client;

#[tokio::main]
async fn main() -> Result<(), s3::Error> {
    let args: Vec<String>=std::env::args().collect();
    let source_path = parse_s3_uri(&args[1]);
    let dest_path=parse_s3_uri(&args[2]);
    println!("Source Path - {:?}\nDestination - {:?}",source_path,dest_path);
    let config = aws_config::load_from_env().await;
    let client = s3::Client::new(&config);
    copy(client, source_path, dest_path).await;
    Ok(())
}

async fn copy(client: Client,source_path: S3Path,dest_path: S3Path) {
    let source_paths = list_objects(&source_path, &client).await.unwrap();
    println!("Objects to copy ......{:?}",source_paths);
    for sp in source_paths {
        let mut sp_w_bucket = String::new();
        sp_w_bucket.push_str(&source_path.bucket);
        sp_w_bucket.push_str("/");
        sp_w_bucket.push_str(&sp);
        let mut dest_key=  String::from(&dest_path.key);
        let dest_vec: Vec<&str> =sp.split("/").collect();
        dest_key.push_str(dest_vec[dest_vec.len()-1]);
        let cp=client.copy_object().copy_source(&sp_w_bucket).bucket(&dest_path.bucket).key(&dest_key).send().await.unwrap();
        println!("{:?}",cp);
    }
}

async fn list_objects(s3_path: &S3Path,client: &Client) -> Result<Vec<String>,aws_sdk_s3::Error> {
    let sourcelist= client.list_objects().bucket(&s3_path.bucket).prefix(&s3_path.key).send().await?;
    let mut paths: Vec<String> = Vec::new(); 
    for o in sourcelist.contents().unwrap() {
        let key=o.key().unwrap();
        paths.push(key.to_owned());
    }
    Ok(paths)
}

fn parse_s3_uri(path: &str) -> S3Path {
    let v: Vec<&str> = path.split('/').collect();
    let bucket = v[2].to_owned();
    let mut key = String::new();
    for k in &v[3..] {
        key.push_str(k);
        key.push_str("/");
    }
    S3Path {bucket,key}

}

#[derive(Debug)]
struct S3Path {
    bucket: String,
    key: String
}