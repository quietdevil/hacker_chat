use chrono::{Datelike, Timelike, Utc};
use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
};

enum ServerEvent {
    NewClient { id: u32, stream: TcpStream },
    Message(Message),
    Disconnect { client_id: u32 },
}

#[derive(Debug, Clone)]
struct Message {
    id: u32,
    msg: String,
}

struct Client {
    id: u32,
    tx: Sender<Message>,
}

pub struct Server {
    connections: Vec<Client>,
    tx: Sender<ServerEvent>,
    rx: Receiver<ServerEvent>,
}

impl Server {
    pub fn build() -> Server {
        let (tx, rx) = channel();
        let vec_cons = Vec::new();
        Server {
            connections: vec_cons,
            tx,
            rx,
        }
    }

    pub fn run(&mut self, ipaddr: Ipv4Addr, port: u16) {
        let addr = SocketAddr::new(std::net::IpAddr::V4(ipaddr), port);
        let listener = TcpListener::bind(addr).expect("Trouble with port");

        let tx = self.tx.clone();

        thread::spawn(move || {
            let mut id = 0;
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        tx.send(ServerEvent::NewClient { id: id, stream }).unwrap();
                    }
                    Err(err) => panic!("{}", err),
                }
                id += 1;
            }
        });

        self.event_loop();
    }

    fn event_loop(&mut self) {
        loop {
            match self.rx.recv() {
                Ok(event) => match event {
                    ServerEvent::NewClient { id, stream } => {
                        let tx = self.tx.clone();
                        let client_tx = Self::spawn_client(id, stream, tx);
                        self.connections.push(Client { id, tx: client_tx });
                    }
                    ServerEvent::Disconnect { client_id } => self.send_all(Message {
                        id: client_id,
                        msg: pretty_message("Hacker leave chat"),
                    }),
                    ServerEvent::Message(msg) => self.send_all(msg),
                },
                Err(err) => println!("{err}"),
            }
        }
    }

    fn spawn_client(id: u32, stream: TcpStream, server_tx: Sender<ServerEvent>) -> Sender<Message> {
        let (tx, rx) = channel::<Message>();

        let stream_reader = stream.try_clone().unwrap();
        let mut stream_writer = stream.try_clone().unwrap();

        let server_tx = server_tx.clone();

        thread::spawn(move || {
            let mut buf = BufReader::new(&stream_reader);

            loop {
                let mut msg = String::new();

                match buf.read_line(&mut msg) {
                    Ok(0) => server_tx
                        .send(ServerEvent::Disconnect { client_id: id })
                        .unwrap(),
                    Ok(_) => server_tx
                        .send(ServerEvent::Message(Message {
                            id,
                            msg: pretty_message(&msg),
                        }))
                        .unwrap(),
                    Err(_) => server_tx
                        .send(ServerEvent::Disconnect { client_id: id })
                        .unwrap(),
                };
            }
        });

        thread::spawn(move || {
            while let Ok(val) = rx.recv() {
                if id == val.id {
                    continue;
                }

                if let Err(err) = write!(&mut stream_writer, "{}", val.msg) {
                    println!("{}", err);
                    break;
                }
            }
        });

        tx
    }

    fn send_all(&mut self, msg: Message) {
        self.connections
            .retain(|client| match client.tx.send(msg.clone()) {
                Ok(_) => true,
                Err(_) => false,
            });
    }
}

fn pretty_message(msg: &str) -> String {
    let time_now = Utc::now();
    format!(
        "[{}-{}-{} {}:{}:{}] {}",
        time_now.year(),
        time_now.month(),
        time_now.day(),
        time_now.hour(),
        time_now.minute(),
        time_now.second(),
        msg
    )
}
