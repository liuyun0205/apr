fn main() {
    use std::io;
    use std::io::prelude::*;

     let stdin = io::stdin();
     let mut lc = 1;
     let mut _str_len = 0;
     for line in stdin.lock().lines() {
     	match lc  {
     		1 => {
     			_str_len = line.unwrap().trim().parse::<usize>().unwrap();
     		},
     		2 => {
     			let s = line.unwrap();
     			let mut num = 0;
     			let mut chars = s.chars();
     			let mut prev_ch = '0';
     			for ch in chars {
     				match ch {
	     				'1' => {
	     					num += 1;
	     				},
	     				'0' if prev_ch == '0' => {
	     					// consecutive zeros represent a zero digit
	     					print!("0");
	     				}
	     				'0' => {
	     					// separator between digits
	     					print!("{}", num);
	     					num = 0;
	     				}
	     				_ => panic!("SOS"),
     				}
     				prev_ch = ch;
     			}
     			// after processing, if there's a trailing run of ones, print it as last digit
     			if num > 0 || prev_ch == '1' {
	     			println!("{}", num);
     			} else {
     				println!();
     			}
     		}
     		_ => {
     			panic!("SOS");
     		}
     	}
     	lc += 1;
     }
}