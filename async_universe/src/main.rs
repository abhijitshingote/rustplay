use std::io::BufRead;
use std::io::Write;
use std::thread;
use std::fs;
use std::io::BufReader;
use std::time::Duration;
use std::sync::{Arc,Mutex};
use std::collections::HashMap;


#[tokio::main]
async fn main() {
    println!("Hello, Rad!!!!");
    let counter=Arc::new(Mutex::new(0));
    let f=fs::File::open("allied_PHCS_47563.csv").expect("No file found!");
    let buffer = BufReader::new(f);
    let mut handles= Vec::new();
    for l in buffer.lines() {
        let c = counter.clone();
        let handle =tokio::spawn( async move {
            thread::sleep(Duration::from_millis(1000));
            let l=l.unwrap();
            let mut line_split = l.split(',');
            line_split.next();            
            // let mut fw = fs::File::create(format!("billing_code={}",line_split.next().unwrap())).unwrap();
            let mut fw = fs::OpenOptions::new().create(true).write(true).append(true).open(format!("billing_code={}",line_split.next().unwrap())).unwrap();
            fw.write(format!("{}\n",l).as_bytes()).unwrap();
            let mut c=c.lock().unwrap();
            *c+=1;
            println!("{}",&l);
            println!("Counter {}",c);
          });
        handles.push(handle);
    }
    for handle in handles {
        handle.await.unwrap();
    }
}
