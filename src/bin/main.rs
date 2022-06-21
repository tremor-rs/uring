use clap::Parser;
use env_logger::Env;
use openraft::Raft;
use uring3::{
    network::raft_network_impl::ExampleNetwork, start_example_raft_node, store::ExampleStore,
    ExampleTypeConfig,
};

pub type ExampleRaft = Raft<ExampleTypeConfig, ExampleNetwork, ExampleStore>;

#[derive(Parser, Clone, Debug)]
#[clap(author, version, about, long_about = None)]
pub struct Opt {
    #[clap(long)]
    pub id: u64,

    #[clap(long)]
    pub http_addr: String,
}

#[async_std::main]
async fn main() -> std::io::Result<()> {
    // Setup the logger
    env_logger::init_from_env(Env::default().default_filter_or("info"));

    // Parse the parameters passed by arguments.
    let options = Opt::parse();

    start_example_raft_node(options.id, options.http_addr).await
}
