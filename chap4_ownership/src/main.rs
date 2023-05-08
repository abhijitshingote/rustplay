use std::str::FromStr;

fn main() {
    let s1: String= String::from("Hello Radhika!");
    let s2: String= pass_ownership_without_reference(s1);
    println!("{}",s2);
    // println!("{}",s1);

    let mut my_mutable_string: String = String::from("I am mutable");
    pass_with_reference(&mut my_mutable_string);
    println!("{}",my_mutable_string);
}

fn pass_ownership_without_reference(mut s: String) -> String {
    s.push_str("Baba and Mama!");
    s
}

// Not only does the variable, that needs to be modified, needs to be mutable but also the refernce needs to be mutable
fn pass_with_reference(ref_s: &mut String) {
    ref_s.push_str("I was changed");
}