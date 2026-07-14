use blinq_server::scripts::create_super_admin; 

#[tokio::main]
async fn main() {
    if let Err(e) = create_super_admin().await {
        eprintln!("Error: {e}");
        std::process::exit(1);
    }
}