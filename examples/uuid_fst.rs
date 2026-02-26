use bumpalo::Bump;
use fst::raw::Builder;
use uuid::Uuid;

fn main() {
    let num_keys: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(1_000_000);

    eprintln!("Generating {} UUID keys...", num_keys);
    let mut keys: Vec<String> = Vec::with_capacity(num_keys);
    for _ in 0..num_keys {
        keys.push(format!("UUID={}", Uuid::new_v4()));
    }

    eprintln!("Sorting keys...");
    keys.sort();
    keys.dedup();

    eprintln!("Building FST with {} unique keys...", keys.len());
    let bump = Bump::new();
    let mut builder = Builder::memory(&bump);
    for key in &keys {
        builder.add(key.as_bytes()).unwrap();
    }
    let fst = builder.into_fst();

    eprintln!("FST size: {} bytes ({:.2} MB)", fst.as_bytes().len(), fst.as_bytes().len() as f64 / (1024.0 * 1024.0));
    eprintln!("Bump arena allocated: {} bytes ({:.2} MB)", bump.allocated_bytes(), bump.allocated_bytes() as f64 / (1024.0 * 1024.0));
    eprintln!("Done.");
}
