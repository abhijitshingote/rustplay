// fn main() {
//     println!("Hello, world!");
//     let s=String::from("hello!!!!");
//     println!("{}",s);
//     let a=&s;
//     println!("{}",a);
//     // let b: f64=5.0;
//     // let mut _c=b+1.0;
// }


// fn main() {
//     let s1 = String::from("hello");

//     let len = calculate_length(s1);

//     println!("The length of '{}' is {}.", s1, len);
// }

// fn calculate_length(s: &String) -> usize {
//     s.len()
// }


struct User {
    active: bool,
    // username: &str,
    username: String,
    email: &String,
    sign_in_count: u64,
    password: String
}

fn main() {
    let em=String::from("some@asdf");
    let uname=String::from("some@asdf");
    let pass: String=String::from("gibberish");
    let user1 = User {
        email: &em,
        username: uname,
        active: true,
        sign_in_count: 1,
        password: pass
    };

    println!("{}",em);
//     // let user2 = user1;
//     let user2 = User {
//         email: user1.email,
//         username: user1.username,
//         active: false,
//         sign_in_count: 2,
//         password: pass
//     };
}