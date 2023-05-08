use std::env;
fn main() {
    println!("Hello, Radhika!");
    let config=get_args();
    println!("Looking in file:{}  for:{}",config.filename,config.searchstring);
}

fn get_args() -> Config {
    let args: Vec<String>=env::args().collect();
    let filename=args[1].clone();
    let searchstring=args[2].clone();
    Config {filename, searchstring}
}

struct Config {
    filename: String,
    searchstring: String
}