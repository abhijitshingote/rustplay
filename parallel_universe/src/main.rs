use std::io::BufRead;
use std::io::Write;
use std::thread;
use std::fs;
use std::io::BufReader;
use std::thread::JoinHandle;
use std::time::Duration;
fn main() {
    println!("Hello, Rad!!!!");
    let f=fs::File::open("allied_PHCS_47563.csv").expect("No file found!");
    let mut buffer = BufReader::new(f);
    let mut handles: Vec<JoinHandle<()>> = Vec::new();
    for l in buffer.lines() {
        let mut handle =thread::spawn(move || {
            thread::sleep(Duration::from_millis(1000));
            let l=l.unwrap();
            let mut line_split = l.split(',');
            line_split.next();
            println!("{}",&l);
            let mut fw = fs::File::create(format!("billing_code={}",line_split.next().unwrap())).unwrap();
            fw.write(l.as_bytes());
            
          });
        handles.push(handle);
    }
    for handle in handles {
        handle.join().unwrap();
    }
}
