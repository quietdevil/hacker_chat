use std::net::Ipv4Addr;

use hacker_chat::server::Server;

fn main() {
    let ipaddr = Ipv4Addr::new(127, 0, 0, 1);
    let port = 8080;

    Server::build().run(ipaddr, port);
}
