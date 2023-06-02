use polars::prelude::*;
use std::fs;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::time::Instant;
use tokio::sync::Semaphore;
use tokio::task::JoinHandle;


#[tokio::main]
async fn main() {
    println!("Hello, Rad!!!!");
    let now = Instant::now();
    // let counter = Arc::new(Mutex::new(0));
    let input_jsonl = "in_network_116mb.jsonl";
    let output: Vec<&str>=input_jsonl.split('.').collect();
    let output = output[0];
    let f = fs::File::open(input_jsonl).expect("No file found!");
    let buffer = BufReader::new(f);
    let mut counter: i32 = 0;
    let lanes = Semaphore::new(1);
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    for l in buffer.lines() {
        counter += 1;
        let _ =lanes.acquire().await.unwrap();
        let handle=tokio::spawn(async move {
            let l = l.unwrap();
            let idx = l.find(r#""billing_code""#).unwrap();
            let idx_bc_1 = l[(idx + 14)..].find('"').unwrap() + idx + 14 + 1;
            let idx_bc_2 = l[(idx_bc_1 + 1)..].find('"').unwrap() + idx_bc_1 + 1;
            let billing_code = &l[idx_bc_1..idx_bc_2];
            // println!("Billing Code : {}", billing_code);
            write_file(output,&l, billing_code, counter).await;
        });
        handles.push(handle);
    }
    for handle in handles {
        let _= handle.await;
    }
    let elapsed_time = now.elapsed();
    println!(
        "Running {} file took {:.10} mins.",input_jsonl,
        (elapsed_time.as_secs_f32()/60_f32 )
    );

}

async fn write_file(output: &str,line: &String, billing_code: &str, counter: i32) {
    let output_dir = format!("{}/billing_code={}", output,billing_code);
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
        .finish().unwrap().collect();
    // println!("{:?}",df);
    match df {
        Ok(mut df) => {
            let _ =ParquetWriter::new(fw).with_compression(ParquetCompression::Snappy).finish(&mut df).unwrap();
        }
        Err(e) => println!("ERROR\n{}\n{}",e,line)
    };
    fs::remove_file(&temp_file).unwrap();
    // let _w = ParquetWriter::new(fw).finish(&mut df);
}
