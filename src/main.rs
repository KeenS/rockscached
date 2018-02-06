extern crate rocksdb;
extern crate chrono;
extern crate bytes;
extern crate futures;
extern crate tokio_core;
extern crate tokio_io;
extern crate tokio_proto;
extern crate tokio_service;
#[macro_use]
extern crate nom;

use bytes::BytesMut;
use futures::{Future, IntoFuture};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::{Encoder, Decoder, Framed};
use tokio_proto::TcpServer;
use tokio_proto::pipeline::ServerProto;
use tokio_service::Service;
use rocksdb::DB;
use chrono::Utc;

use std::path::Path;
use std::fmt;
use std::io;
use std::str;
use std::str::FromStr;
use nom::{digit, IResult, alphanumeric, space};

type Result<T> = ::std::result::Result<T, rocksdb::Error>;

#[derive(Debug, Clone, PartialEq, Eq)]
struct Value {
    pub data: Vec<u8>,
    pub flags: u32,
    pub exptime: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Command {
    Set { key: Vec<u8>, value: Value },
    Get { keys: Vec<Vec<u8>> },
    Delete { key: Vec<u8> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum CommandRet {
    Stored,
    Got(Vec<(Vec<u8>, Value)>),
    Deleted,
    NotFound,
}

impl CommandRet {
    fn to_vec(&self) -> Vec<u8> {
        use self::CommandRet::*;
        match *self {
            Stored => b"STORED\r\n".to_vec(),
            Got(ref results) => {
                use std::str::from_utf8;
                let mut ret = Vec::new();
                for &(ref key, ref value) in results {
                    ret.extend(
                        format!(
                            "VALUE {} {} {}\r\n",
                            from_utf8(key).unwrap(),
                            value.flags,
                            value.data.len()
                        ).as_bytes(),
                    );
                    ret.extend(&value.data);
                    ret.extend(b"\r\n");
                }
                ret.extend(b"END\r\n");
                ret
            }
            Deleted => b"DELETED\r\n".to_vec(),
            NotFound => b"NOT_FOUND\r\n".to_vec(),
        }
    }
}

impl fmt::Display for CommandRet {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use std::str::from_utf8;
        write!(f, "{}", from_utf8(&self.to_vec()).unwrap())
    }
}


fn encode_be(b: u32) -> [u8; 4] {
    [
        ((b >> 24) & 0xff) as u8,
        ((b >> 16) & 0xff) as u8,
        ((b >> 8) & 0xff) as u8,
        (b & 0xff) as u8,
    ]
}

fn decode_be(bytes: [u8; 4]) -> u32 {
    (bytes[0] as u32) << 24 + (bytes[1] as u32) << 16 + (bytes[2] as u32) << 8 + bytes[3]
}

impl Value {
    fn pack(self) -> Vec<u8> {
        let mut data = self.data;
        data.extend_from_slice(&encode_be(self.flags));
        data.extend_from_slice(&encode_be(self.exptime as u32));
        data
    }

    fn from_vec(mut data: Vec<u8>) -> Value {
        let len = data.len();
        assert!(len > 8);
        let flags = decode_be([data[len - 8], data[len - 7], data[len - 6], data[len - 5]]);
        let exptime = decode_be([data[len - 4], data[len - 3], data[len - 2], data[len - 1]]);
        let exptime = exptime as i64;
        data.truncate(len - 8);
        Value {
            data,
            flags,
            exptime,
        }
    }
}

struct Engine {
    db: DB,
}

impl Engine {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = DB::open_default(path)?;
        Ok(Self { db: db })
    }

    pub fn exec(&self, cmd: Command) -> Result<CommandRet> {
        match cmd {
            Command::Set { key, value } => {
                let () = self.set(key, value)?;
                Ok(CommandRet::Stored)
            }
            Command::Get { keys } => {
                let v = self.get(keys)?;
                Ok(CommandRet::Got(v))
            }
            Command::Delete { key } => {
                if self.delete(key)? {
                    Ok(CommandRet::Deleted)
                } else {
                    Ok(CommandRet::NotFound)
                }
            }
        }
    }

    /// create, update or delete the kv pair
    fn set(&self, key: Vec<u8>, value: Value) -> Result<()> {
        if value.exptime < 0 {
            self.db.delete(&key)
        } else {
            self.db.put(&key, &value.pack())
        }
    }

    /// find data and collect only found data
    fn get(&self, keys: Vec<Vec<u8>>) -> Result<Vec<(Vec<u8>, Value)>> {
        let mut ret = Vec::new();
        for key in keys {
            match self.db.get(&key)? {
                None => (),
                Some(v) => {
                    let entry = Value::from_vec(v.to_vec());
                    if entry.exptime == 0 {
                        ret.push((key, entry))
                    } else {
                        let now = Utc::now();
                        if entry.exptime < now.timestamp() {
                            self.db.delete(&key)?
                        } else {
                            ret.push((key, entry))
                        }
                    }
                }
            }

        }
        Ok(ret)
    }

    fn delete(&self, key: Vec<u8>) -> Result<bool> {
        let exists = self.db.get(&key)?.is_some();
        self.db.delete(&key).map(|()| exists)
    }
}


struct MemcachedServer {
    engine: Engine,
}

impl Service for MemcachedServer {
    type Request = Command;
    type Response = CommandRet;
    type Error = io::Error;
    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, cmd: Self::Request) -> Self::Future {
        Box::new(
            self.engine
                .exec(cmd)
                .map_err(|err| io::Error::new(io::ErrorKind::Other, err))
                .into_future(),
        )
    }
}


struct MemcachedCodec;
impl Encoder for MemcachedCodec {
    type Item = CommandRet;
    type Error = io::Error;

    fn encode(&mut self, cmd: Self::Item, buf: &mut BytesMut) -> io::Result<()> {
        Ok(buf.extend(cmd.to_vec()))
    }
}

impl Decoder for MemcachedCodec {
    type Item = Command;
    type Error = io::Error;

    fn decode(
        &mut self,
        buf: &mut BytesMut,
    ) -> ::std::result::Result<Option<Self::Item>, Self::Error> {
        let (read, cmd) = match parse_cmd(buf) {
            IResult::Done(rest, cmd) => {
                let read = buf.len() - rest.len();
                (read, cmd)
            }
            IResult::Incomplete(_) => return Ok(None),
            IResult::Error(_) => {
                return Err(io::Error::new(io::ErrorKind::Other, "invalid protocol"))
            }
        };
        buf.advance(read);
        Ok(Some(cmd))
    }
}

named!(parse_cmd<&[u8], Command>, alt!(parse_set | parse_get | parse_delete));
named!(parse_set<Command>, do_parse!(
    tag!(b"set") >> space >>
        key: alphanumeric >> space >>
        flags: parse_u32 >> space >>
        exptime: parse_i64 >> space >>
        len: parse_u32 >>
        tag!(b"\r\n") >>
        data: take!(len) >>
        tag!(b"\r\n") >>
        ({
            let data: &[u8] = data;
            Command::Set {
                key: key.to_vec(),
                value: Value {
                    flags: flags,
                    exptime: exptime,
                    data: data.to_vec(),
                }
            }})
));

named!(parse_get<Command>, do_parse!(
    tag!(b"get") >>
        space >>
        keys: separated_nonempty_list!(space, alphanumeric) >>
        tag!(b"\r\n") >>
        (Command::Get {
            keys: keys.iter().map(|k| k.to_vec()).collect(),
        })
));

named!(parse_delete<Command>, do_parse!(
    tag!(b"delete") >>
        space >>
        key: alphanumeric >>
        tag!(b"\r\n") >>
        (Command::Delete {
            key: key.to_vec(),
        })
));

named!(parse_u32<u32>, map_res!(map_res!(digit, str::from_utf8), FromStr::from_str));
named!(parse_i64<i64>, map_res!(map_res!(recognize!(
    do_parse!(opt!(tag!(b"-")) >> digit >> ())),
                                         str::from_utf8),
                                FromStr::from_str));

#[test]
fn test_parser() {
    assert_eq!(parse_cmd(b"delete key\r\n"),
               IResult::Done(&b""[..], Command::Delete{key: b"key".to_vec()}));
    assert_eq!(parse_cmd(b"get key1 key2\r\n"),
               IResult::Done(&b""[..], Command::Get{keys: vec![b"key1".to_vec(), b"key2".to_vec()]}));
    assert_eq!(parse_cmd(b"set key 1 0 5\r\nhello\r\n"),
               IResult::Done(&b""[..], Command::Set {key: b"key".to_vec(), value: Value {
                   exptime: 0,
                   flags: 1,
                   data: b"hello".to_vec(),
               }}));
}


struct MemcachedProto;
impl<T: AsyncRead + AsyncWrite + 'static> ServerProto<T> for MemcachedProto {
    // For this protocol style, `Request` matches the `Item` type of the codec's `Decoder`
    type Request = Command;

    // For this protocol style, `Response` matches the `Item` type of the codec's `Encoder`
    type Response = CommandRet;

    // A bit of boilerplate to hook in the codec:
    type Transport = Framed<T, MemcachedCodec>;
    type BindTransport = ::std::result::Result<Self::Transport, io::Error>;
    fn bind_transport(&self, io: T) -> Self::BindTransport {
        Ok(io.framed(MemcachedCodec))
    }
}


fn main() {
    // Specify the localhost address
    let addr = "0.0.0.0:12345".parse().unwrap();

    // The builder requires a protocol and an address
    let server = TcpServer::new(MemcachedProto, addr);

    // We provide a way to *instantiate* the service for each new
    // connection; here, we just immediately return a new instance.
    server.serve(|| {
        Ok(MemcachedServer {
            engine: Engine::new("path/for/rocksdb/storage").unwrap(),
        })
    });
}
