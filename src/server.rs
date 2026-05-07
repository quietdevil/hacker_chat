use std::{
    io::{BufRead, BufReader, Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    sync::{
        Arc, Mutex,
        mpsc::{Receiver, Sender, channel},
    },
    thread,
};

enum Event {
    Join,
    M(Message),
    Quit(String),
}

#[derive(Debug, Clone)]
struct Message {
    id: u32,
    msg: String,
}

struct Connect {
    id: u32,
    send_channel: Sender<Event>,
}

impl Connect {
    fn build(id: u32, send_channel: Sender<Event>) -> Connect {
        Connect {
            id: id,
            send_channel: send_channel,
        }
    }

    fn new(self, stream: TcpStream) -> Sender<Message> {
        let (sender, receiver) = channel::<Message>();

        let stream_reader = stream.try_clone().unwrap();

        let id = self.id;

        thread::spawn(move || {
            loop {
                let mut msg = String::new();
                let mut buf = BufReader::new(&stream_reader);

                // if close connection loop break
                if let Ok(end) = buf.read_line(&mut msg) {
                    // EOF
                    if end == 0 {
                        break;
                    }
                };
                match self.send_channel.send(Event::M(Message {
                    id: id,
                    msg: msg.clone(),
                })) {
                    Ok(()) => println!("receive message id {}", id),
                    Err(err) => eprintln!("{err}"),
                }
            }
        });

        let mut stream_writter = stream.try_clone().unwrap();

        thread::spawn(move || {
            while let Ok(val) = receiver.recv() {
                println!("{}", val.msg);
                if id == val.id {
                    continue;
                }

                if let Err(err) = write!(&mut stream_writter, "{}", val.msg) {
                    println!("{}", err);
                    break;
                }
            }
        });
        sender
    }
}

pub struct Server {
    connections: Arc<Mutex<Vec<Sender<Message>>>>,
}

impl Server {
    pub fn build() -> Server {
        let vec_cons = Arc::new(Mutex::new(Vec::new()));
        Server {
            connections: vec_cons,
        }
    }

    pub fn run(self, ipaddr: Ipv4Addr, port: u16) {
        let (sender_client, receiver_server) = channel();

        let addr = SocketAddr::new(std::net::IpAddr::V4(ipaddr), port);
        let listener = TcpListener::bind(addr).expect("Trouble with port");

        let vec_conns = Arc::clone(&self.connections);

        thread::spawn(move || {
            let mut id = 0;
            for stream in listener.incoming() {
                match stream {
                    Ok(stream) => {
                        let send = sender_client.clone();
                        let client = Connect::build(id, send);
                        id += 1;
                        self.add_connect(stream, client);
                    }
                    Err(err) => panic!("{}", err),
                }
            }
        });

        loop {
            match receiver_server.recv() {
                Ok(event) => match event {
                    Event::Join => println!(""),
                    Event::M(message) => {
                        let mut share_msg = vec_conns.lock().unwrap();
                        println!("Server receive message - {}", message.msg);

                        share_msg.retain(|tx| match tx.send(message.clone()) {
                            Ok(_) => true,
                            Err(err) => {
                                println!("{}", err);
                                false
                            }
                        });
                    }
                    Event::Quit(msg) => println!("{}", msg),
                },
                Err(err) => println!("{}", err),
            }
        }
    }

    fn add_connect(&self, stream: TcpStream, client: Connect) {
        self.connections.lock().unwrap().push(client.new(stream));
    }
}
