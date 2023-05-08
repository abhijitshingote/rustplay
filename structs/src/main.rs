use std::io;

fn main() {
    println!("Hello, world!");
    let mystr = String::from("Hello, world!");
    let strref = &mystr;
    println!("{}", &strref);
    let input = get_user_input(&mystr);
    println!("{}", input);
    // A type can be mutable and therefore all its fields are mutable, no partial mutable fields allowed
    let mut name = String::from("Abhijit Shingote");
    let mut usr1 = User {
        name: name,
        age: 38,
    };
    println!("{} is {} years old", usr1.name, usr1.age);
    usr1.name = String::from("Radhika Shingote");
    usr1.age = 6;
    usr1.name.insert_str(0, "King Julien");
    println!("{} is {} years old", usr1.name, usr1.age);

    fn modify_user(user: &mut User) {
        user.name.insert_str(0, "Modified");
    }
    modify_user(&mut usr1);
    println!("{} is {} years old", usr1.name, usr1.age);
}

fn get_user_input(mystr: &String) -> String {
    println!("burr{}", mystr);
    let mut input = String::new();
    io::stdin().read_line(&mut input);
    let length = input.len();
    input.insert_str(length - 1, mystr);
    input
}

struct User {
    name: String,
    age: i32,
}
// A -> ptrA -> "asda"`
// B ->ptrB -> ptrA
// *B -> ptrB -> ptrA -> "asda"
