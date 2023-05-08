fn main() {
    println!("Hello, world!");

    // Fahrenheit to Celsius
    fn f_to_c(f: i32) {
    let f: i32 = f;
    let c: f64 = (f as f64 - 32.0 ) / 1.8 ;
    println!("{} in Fahrenheit equals {} in Celcius",f,c); 
    }

    let mut f: i32=100;
    loop {
        
        f_to_c(f);
        f=f-20;
        if f==0 {
            break;
        }

    }
    


}
