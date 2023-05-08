use std::fs::File;
// fn main() {
//     println!("Hello, world!");
//     let v=vec![1,2,3];
//     // println!("{}",v[4]);

//     let greeting_file_result = File::open("hello.txt");

//     let greeting_file = match greeting_file_result {
//         Ok(file) => file,
//         // Err(error) => File::create("some").unwrap(),
//         Err(error) => panic!("Problem opening the file: {:?}", error)
//     };

    
// }


// Important Rules
// 1. When accessing reference to a slice or a vector,
    // a. if you index an object within the list such a=&[i32] and then a[0], first pointer &a is dereferenced to realize a and then the value at a[0] is used -copy trait
    // b. however if you iterate on the pointer to the slice, such a=&[i32] and then for i in a, type of i is a reference to the object itself, and to get value, do for &i in a
// 2. Its ok to have mutable and immutable references in the same scope, what isnt allowed is an immutable ref followed by mutable and then accessing the immutable reference
fn largest(list: &[i32]) -> i32 {
    let a =list;
    let i= &a[0];
    let mut largest = list[0];

    for &item in list.iter() {
        if item > largest {
            largest = item;
        }
    }

    largest
}

fn main() {
    let number_list = vec![34, 50, 25, 100, 65];

    let result = largest(&number_list);
    println!("The largest number is {}", result);

    let number_list = vec![102, 34, 6000, 89, 54, 2, 43, 8];

    let result = largest(&number_list);
    println!("The largest number is {}", result);
}