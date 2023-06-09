use polars::frame::explode;
use polars::prelude::*;
use std::fs;
// use std::io::BufReader;
use std::io::Cursor;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;
use clap::Parser;

use aws_sdk_s3::Client;
use tokio::io::{AsyncBufReadExt, BufReader};
use std::{process};
use std::sync::Arc;

#[tokio::main]
async fn main() {
    println!("Hello, Rad!!!!");
    // tokio::runtime::Builder::
    let now = Instant::now();
    // let counter = Arc::new(Mutex::new(0));
    let args = Opt::parse();
    // let output=args.output.clone();
    let output_arc =Arc::new(args);
    // let input_jsonl = "in_network_116mb.jsonl";
    // let output: Vec<&str>=input_jsonl.split('.').collect();
    // let output = output[0];
    let config = aws_config::load_from_env().await;
    let client = Client::new(&config);
    // // copy(&client, source_path, dest_path).await;
    let path = parse_s3_uri(&output_arc.input);
    let in_network_key = format!("{}/in_network.jsonl",&path.key);
    let prov_ref_key = format!("{}/provider_references.jsonl",&path.key);
    println!("{:?}", path);
    // println!("{:?}", in_network_key);
    // println!("{:?}",prov_ref_key);
    
    let prov_ref_obj = client
        .get_object()
        .bucket(&path.bucket)
        .key(&prov_ref_key)
        .send()
        .await
        .expect("Issues get object!");

    let prov_df = JsonLineReader::new(Cursor::new(prov_ref_obj.body.collect().await.unwrap().into_bytes())).finish().unwrap();
    let prov_df = prov_df.explode(["provider_groups"]).unwrap().unnest(["provider_groups"]).unwrap();
    let prov_df = prov_df.explode(["npi"]).unwrap().unnest(["tin"]).unwrap();
    // let prov_df=prov_df.rename("provider_group_id", "provider_references").unwrap();
    // println!("{:?}",prov_df);

    let prov_df_ref=Arc::new(prov_df);

    let obj = client
        .get_object()
        .bucket(&path.bucket)
        .key(&in_network_key)
        .send()
        .await
        .expect("Issues get object!");
    let buf_reader = BufReader::new(obj.body.into_async_read());
    let mut lines = buf_reader.lines();
    // let l =lines.next_line()
    let mut counter: i32 = 0;
    let lanes = Arc::new(Semaphore::new(1));
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    for _ in 0..output_arc.num_lines {
        counter += 1;
        
        let permit =lanes.clone().acquire_owned().await.unwrap();
        if counter%1==0 {
            println!("Line {} started - Got Semaphore,Time elapsed {} mins",&counter,(now.elapsed().as_secs_f32()/60_f32 ));
        }
        let l = lines.next_line().await.unwrap_or_else(|err| {process::exit(0)});
        let l=l.unwrap_or_else( || {
            println!("Maybe end of file!\nRunning file took {:.10} mins.",(now.elapsed().as_secs_f32()/60_f32 ));
            process::exit(1);});
        let output_clone=output_arc.clone();
        let prov_df_ref_clone = prov_df_ref.clone();
        let handle=tokio::spawn(async move {        
            let idx = l.find(r#""billing_code""#).unwrap();
            let idx_bc_1 = l[(idx + 14)..].find('"').unwrap() + idx + 14 + 1;
            let idx_bc_2 = l[(idx_bc_1 + 1)..].find('"').unwrap() + idx_bc_1 + 1;
            let billing_code = &l[idx_bc_1..idx_bc_2];
            // println!("Billing Code : {}", billing_code);
            write_file(&output_clone,&l, billing_code, counter,prov_df_ref_clone).await;
            drop(permit);
            if counter%1==0 {
                println!("Line {} completed - Dropped Semaphore Semaphore,Time elapsed {} mins",&counter,(now.elapsed().as_secs_f32()/60_f32 ));
            }
        });
        handles.push(handle);
    }
    for handle in handles {
        let _= handle.await;
    }
    let elapsed_time = now.elapsed();
    println!(
        "Running {} file took {:.10} mins.",&output_arc.input,
        (elapsed_time.as_secs_f32()/60_f32 )
    );

}

async fn write_file(config: &Arc<Opt>,line: &String, billing_code: &str, counter: i32, prov_df_ref_clone: Arc<DataFrame>) {
    let output_dir = format!("{}/billing_code={}", config.output,billing_code);
    fs::DirBuilder::new()
        .recursive(true)
        .create(&output_dir)
        .unwrap();
    let fw = fs::OpenOptions::new()
        .create(true)
        .write(true)
        .open(format!(
            "{}/{}.parquet",
            output_dir,
            counter
        ))
        .unwrap();
    let f1 = Field::new("negotiation_arrangement", DataType::Utf8);
    let f2 = Field::new("name", DataType::Utf8);
    let f3 = Field::new("billing_code_type", DataType::Utf8);
    let f4 = Field::new("billing_code", DataType::Utf8);
    let f5 = Field::new("billing_code_type_version", DataType::Utf8);
    let f6 = Field::new("description", DataType::Utf8);
    let f7 = Field::new("billing_class", DataType::Utf8);
    let f8 = Field::new("expiration_date", DataType::Utf8);
    let f9 = Field::new("negotiated_rate", DataType::Float32);
    let f10 = Field::new("negotiated_type", DataType::Utf8);
    let f14=Field::new("service_code",DataType::List(Box::new(DataType::Utf8)));
    let f15=Field::new("billing_code_modifier",DataType::List(Box::new(DataType::Utf8)));
    let f11=Field::new("negotiated_prices_obj",DataType::Struct(vec![f7,f8,f9,f10,f14,f15]));
    let f12=Field::new("negotiated_prices",DataType::List(Box::new(f11.dtype)));
    let f16=Field::new("provider_references",DataType::List(Box::new(DataType::Int64)));
    let f13=Field::new("negotiated_rates_obj",DataType::Struct(vec![f12,f16]));
    let f17=Field::new("negotiated_rates",DataType::List(Box::new(f13.dtype)));
    let temp_file = format!(
        "{}/{}.txt",
        output_dir,
        counter
    );
    fs::write(&temp_file, line).unwrap();
    let schema=Schema::from_iter(vec![f1,f2,f3,f4,f5,f6,f17]);
    // .with_column(f1.name, f1.dtype)
    // let schema=Schema::from(vec![f1,f2]);
    let df = LazyJsonLineReader::new(temp_file.clone())
        // .with_json_format(JsonFormat::JsonLines)
        .with_schema(schema)
        // .infer_schema_len(Some(3))
        // .with_batch_size(3)
        // .with_projection(Some(vec![String::from("negotiated_rates"),String::from("billing_code_type")]))
        .finish().unwrap();
    let df=df.select([col("*")]).explode([col("negotiated_rates")]);
    // let df = df.select([col("*"),col("negotiated_rates").alias("something")]);
    let df = df.unnest(["negotiated_rates"]);
    let df = df.explode([col("provider_references")]);
    // .select(&[col("negotiated_rates")]).collect().unwrap();
    // println!("{:?}",df.collect().unwrap());
    let prov_df=prov_df_ref_clone;
    let owned_prov_df=(*prov_df).clone();
    // let cloned_prov_df=owned_prov_df.clone()
    // let prov_df_lazy_ref= &prov_df.lazy();
    // println!("{:?}",prov_df);
    // let join_df=df.collect().unwrap().inner_join(&prov_df, ["provider_references"], ["provider_group_id"]);

    let join_df=df.inner_join(owned_prov_df.lazy(), col("provider_references"), col("provider_group_id")).collect();
    // .inner_join(&prov_df, ["provider_references"], ["provider_group_id"]);
    // let join_df=join_df.unwrap().clone();
    // println!("{:?}",join_df);
    // let _ =ParquetWriter::new(fw).with_compression(ParquetCompression::Snappy).finish(&mut join_df).unwrap();
    match join_df {
        Ok(mut df) => {
            let _ =ParquetWriter::new(fw).with_compression(ParquetCompression::Snappy).finish(&mut df).unwrap();
        }
        Err(e) => println!("ERROR\n{}\n{}",e,line)
    };
    // fs::remove_file(&temp_file).unwrap();

}



fn parse_s3_uri(path: &str) -> S3Path {
    let v: Vec<&str> = path.split('/').collect();
    let bucket = v[2].to_owned();
    let key = v[3..].join("/");
    S3Path { bucket, key }
}

#[derive(Default, Parser, Debug)]
struct Opt {
    #[clap(short, long)]
    input: String,
    #[clap(short, long)]
    output: String,
    #[clap(short, long)]
    num_lines: i128,
}


#[derive(Debug)]
struct S3Path {
    bucket: String,
    key: String,
}
