fn read_int() -> usize {
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).expect("Failed");
    input.trim().parse::<usize>().unwrap()
}

fn read_int_vec() -> Vec<usize> {
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).expect("read error");
    input.split_whitespace()
        .map(|i| i.parse::<usize>().expect("parse error"))
        .collect::<Vec<usize>>()
}

fn main() {
    let n = read_int();
    let mut vec = read_int_vec();
    // If second line might contain more numbers than needed, take first n-1 (excluding Dima)
    // In problem input, first number is number of people including Dima.
    // The following line contains n-1 numbers (friends). Ensure we only use n-1.
    if vec.len() > n - 1 {
        vec.truncate(n - 1);
    }
    let sum: usize = vec.iter().sum();

    let mut total = 0;
    for i in 1..=5 {
        // total fingers shown = sum of friends + Dima's i
        // counting starts from Dima and counts total modulo n
        // if result is 1 (Dima), he cleans; we want ways where he does NOT clean
        let pos = (sum + i) % n;
        // position 0 corresponds to n, but counting-out uses 1..n with Dima being 1
        let is_dima = pos == 1 % n || (pos == 0 && n == 1);
        // Simpler: counting result k = (sum + i) % n; if k==1 -> Dima. However when modulo yields 0 it means n.
        let result = if pos == 0 { n } else { pos };
        if result != 1 {
            total += 1;
        }
    }
    println!("{}", total);
}