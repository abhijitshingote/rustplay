use std::collections::HashMap;
use std::io;

fn main() {
    // Vector Math
    let mut v=vec![ 8,	68,	1,	100,	38,
    4,	78,	93,	73,	48,
    86,	100,	31,	14,	43];
    v.sort();
    let mut len = v.len();
    len=len/2 as usize;

    
    let median = &v[len];
    println!("The median of the vector  {:?} is  {}",&v,median);
    let mut hm_counter: HashMap<i32,i32>=HashMap::new();
    for e in &v {
        let counter=hm_counter.entry(*e).or_insert(0);
        *counter+=1;
    }
    let mut hm_counter_vec: Vec<(&i32,&i32)>=hm_counter.iter().collect();
    hm_counter_vec.sort_by(|a,b| b.1.cmp(a.1));
    println!("{} appears {} times",hm_counter_vec[0].0,hm_counter_vec[0].1);

    //Pig Latin
    let mut piglatin=String::new();
    println!("Enter a sentence:");
    let mut strr = String::new();
    io::stdin().read_line(&mut strr).expect("failed to read line");

    for sss in strr.split_whitespace() {
        for c in sss.chars() {
            if c=='a' || c=='e' || c=='i'  || c=='o' || c=='u' {
                piglatin.push_str(&format!("{}-hoy ",sss));
                break;
            }
            else {
                piglatin.push_str(&format!("{}-{}ay ",&sss[1..],c));
                break;
            }
        }

    }
    println!("{}",&piglatin);
    }
