use std::{fs::File};
use std::io::{BufReader, BufRead,stdout, Write};
use std::env;
fn main() -> std::io::Result<()> {
    let mut stdout=stdout();
    let mut counter=0;
    let f=File::open(get_path()).expect("file doesnt exist");
    let bf=BufReader::new(f);
    let lines=bf.lines();

    for line in lines {
        let linnn=line?;
        if counter%100000==0 {
        // print!("line# {}:{}",counter,&linnn);
        stdout.flush().unwrap();
        print!("\r{}\r", " ".repeat(46));
        }
        counter+=1;
    }
    println!("Total lines in this file:{}",counter);
    std::io::Result::Ok(())

}

fn get_path() -> String{
    let args: Vec<String> = env::args().collect();
    let file_path = args[1].clone();
    file_path
}